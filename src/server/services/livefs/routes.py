"""What the mount does at one route: list a directory, read, save, delete and rename a file.

The store's routes answer the file tools under two contracts. A plain store
route keeps the bytes it is sent and has no versions of its own, so a file's
version is its content's hash, which the store checks a save against under its
write lock. A ``DbJsonRoute`` renders database rows, versions them itself and
checks a save against that version under its write lock; a
``DbJsonFolderRoute``, one file per row, also deletes and renames them. Each
kind is adapted here once, so the tree asks every route the same questions
(``MountRoute``).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, NamedTuple, Protocol

from ptc_agent.agent.backends import (
    InvalidStoreKeyError,
    ReadOnlyStoreError,
    StoreBackend,
    StoreContentTooLargeError,
    WorkflowsBackend,
)
from ptc_agent.agent.backends.db_json_folder import DbJsonFolderRoute
from ptc_agent.agent.backends.db_json_route import DbJsonRoute, UserDataValidationError
from ptc_agent.agent.backends.langgraph_store import StoreListingIncomplete
from ptc_agent.core.sandbox.livefs_runtime.protocol import INLINE_MAX_BYTES, Refusal

logger = logging.getLogger(__name__)

_STATUS = {
    Refusal.NOT_FOUND: 404,
    Refusal.IS_DIRECTORY: 409,
    Refusal.NOT_DIRECTORY: 409,
    Refusal.EXISTS: 412,
    Refusal.CHANGED: 412,
    Refusal.READ_ONLY: 403,
    Refusal.INVALID: 422,
    Refusal.PRECONDITION_REQUIRED: 428,
    Refusal.TOO_LARGE: 413,
    Refusal.UNAVAILABLE: 503,
}


class LivefsError(Exception):
    """A refusal the daemon turns into an errno and the agent reads as text.
    ``path`` is the sandbox path the agent used, which its outcome is filed by."""

    def __init__(self, code: Refusal, message: str, path: str) -> None:
        super().__init__(message)
        self.code = code
        self.status = _STATUS[code]
        self.message = message
        self.path = path


def not_found(path: str) -> LivefsError:
    return LivefsError(Refusal.NOT_FOUND, f"No such file or directory: {path}", path)


def changed(path: str) -> LivefsError:
    return LivefsError(
        Refusal.CHANGED,
        f"{path} changed since it was read. Read it again and reapply the change.",
        path,
    )


def version_of(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()[:32]


def file_entry(name: str, content: str, *, writable: bool) -> dict[str, Any]:
    """A listing entry for a file whose content the route holds, carried
    along so the daemon need not read it (the tree trims what it may carry)."""
    return {
        "name": name,
        "type": "file",
        "size": len(content.encode()),
        "version": version_of(content),
        "writable": writable,
        "content": content,
    }


def is_root(root_prefix: str, path: str) -> bool:
    return path.rstrip("/") == root_prefix.rstrip("/")


class Saved(NamedTuple):
    version: str
    size: int
    #: The sandbox path the agent knows the file by.
    path: str
    #: What the save changed, for a file that reports that.
    report: str | None
    #: Whether the file now holds exactly the bytes sent, so the sender may
    #: keep them as its content at ``version`` rather than read it back.
    as_sent: bool
    #: The save deleted the file, as an automation's does with ``"status":
    #: "deleted"``, so there is no version or content to hold.
    removed: bool = False


class Removed(NamedTuple):
    #: What the delete changed, for a file that reports that.
    report: str | None


class InlineBudget:
    """How much file content one listing carries: none of a file past the
    per-file limit, and nothing once the listing's total is spent."""

    def __init__(self, total: int) -> None:
        self._left = total

    def take(self, size: int) -> bool:
        """Whether a file of ``size`` bytes is carried, spending it if so."""
        if size > INLINE_MAX_BYTES or size > self._left:
            return False
        self._left -= size
        return True


class MountRoute(Protocol):
    """What the tree asks of a route, at sandbox paths under ``root_prefix``."""

    @property
    def root_prefix(self) -> str: ...

    def is_writable(self, path: str) -> bool: ...

    async def movable(self, path: str) -> bool:
        """Whether a rename may take the file away from here."""

    async def list(self, path: str) -> list[dict[str, Any]] | None:
        """The directory's entries; None where there is no directory. A file's
        ``version`` is the one ``read`` returns with the same bytes, and it may
        carry ``content`` when the route holds it anyway."""

    async def list_tree(
        self, path: str, inline: InlineBudget
    ) -> dict[str, list[dict[str, Any]]] | None:
        """The directory and each one below it, by its path relative to
        ``path``, when the route lists them together; None to list them one
        at a time. Files carry ``content`` as far as ``inline`` allows."""

    async def read(self, path: str) -> tuple[str, str] | None:
        """Content and the version a save must name; None where no file is."""

    async def write(self, path: str, content: str, version: str | None) -> Saved:
        """Save over ``version``, or only where no file is when it is None."""

    async def delete(self, path: str, version: str | None = None) -> Removed | None:
        """Delete the file, only at ``version`` when given; None where there
        was none."""

    async def rename(self, path: str, to: str) -> str | None:
        """Move the file to ``to`` under this route, keeping what it stands
        for, and report it; None to move it as a copy and a delete."""


@contextmanager
def _refusing(path: str) -> Iterator[None]:
    """A route's refusal of a change, as the refusal the daemon answers."""
    try:
        yield
    except ReadOnlyStoreError as exc:
        raise LivefsError(Refusal.READ_ONLY, str(exc), path) from exc
    except StoreContentTooLargeError as exc:
        raise LivefsError(Refusal.TOO_LARGE, str(exc), path) from exc
    except UserDataValidationError as exc:
        if exc.error_type == "version_conflict":
            raise changed(path) from exc
        if exc.error_type == "server_error":
            raise LivefsError(Refusal.UNAVAILABLE, exc.hint, path) from exc
        if exc.error_type == "exists":
            raise LivefsError(Refusal.EXISTS, exc.message, path) from exc
        if exc.error_type == "deleted":
            raise LivefsError(
                Refusal.NOT_FOUND,
                f"{path} was deleted since it was read, so nothing was saved. Writing it "
                "again creates it anew: do that only if the user wants it back.",
                path,
            ) from exc
        raise LivefsError(Refusal.INVALID, exc.message, path) from exc
    except (InvalidStoreKeyError, ValueError) as exc:
        raise LivefsError(Refusal.INVALID, str(exc), path) from exc


class _StoreRoute:
    """Files kept as sent. A directory is listed from one read of the subtree,
    which holds every file's content, so the listing carries it."""

    def __init__(self, route: StoreBackend | WorkflowsBackend) -> None:
        self._route = route
        self.root_prefix = route.root_prefix

    def is_writable(self, path: str) -> bool:
        return self._route.is_writable(path)

    async def movable(self, path: str) -> bool:
        if isinstance(self._route, WorkflowsBackend):
            # A shipped script is writable, as a save forks it, but one no
            # save has forked holds nothing of the user's to delete.
            return await self._route.ais_deletable(path)
        return self._route.is_writable(path)

    async def list(self, path: str) -> list[dict[str, Any]] | None:
        try:
            tree = await self._route.aread_tree(path)
        except StoreListingIncomplete as exc:
            raise LivefsError(Refusal.UNAVAILABLE, str(exc), path) from exc
        if path in tree:
            raise LivefsError(Refusal.NOT_DIRECTORY, f"{path} is a file", path)
        base = path.rstrip("/") + "/"
        entries: dict[str, dict[str, Any]] = {}
        for file_path, content in tree.items():
            if not file_path.startswith(base):
                continue
            name, nested, _ = file_path[len(base) :].partition("/")
            if nested:
                entries.setdefault(name, {"name": name, "type": "dir"})
                continue
            entries[name] = file_entry(
                name, content, writable=self._route.is_writable(file_path)
            )
        if not entries and not is_root(self.root_prefix, path):
            return None
        return sorted(entries.values(), key=lambda e: e["name"])

    async def list_tree(self, path: str, inline: InlineBudget) -> None:
        return None

    async def read(self, path: str) -> tuple[str, str] | None:
        content = await self._route.aread_text(path)
        return None if content is None else (content, version_of(content))

    async def write(self, path: str, content: str, version: str | None) -> Saved:
        def check(current: str | None) -> None:
            # Run by the store under the lock its write holds, on what the
            # write replaces, so a writer landing after the sender's read is
            # caught however close behind it.
            if version is None:
                if current is not None:
                    raise LivefsError(Refusal.EXISTS, f"{path} already exists", path)
            elif current is None or version_of(current) != version:
                raise changed(path)

        with _refusing(path):
            ok = await self._route.awrite_text(path, content, check=check)
        if not ok:
            raise LivefsError(Refusal.UNAVAILABLE, f"{path} was not saved; retry", path)
        return Saved(version_of(content), len(content.encode()), path, None, True)

    async def delete(self, path: str, version: str | None = None) -> Removed | None:
        def check(current: str | None) -> None:
            # Under the lock a save's check runs under, as that one is.
            if version is not None and (current is None or version_of(current) != version):
                raise changed(path)

        with _refusing(path):
            try:
                deleted = await self._route.adelete_text(path, check=check)
                return Removed(None) if deleted else None
            except asyncio.TimeoutError as exc:
                raise LivefsError(
                    Refusal.UNAVAILABLE, f"{path} was not deleted; retry", path
                ) from exc

    async def rename(self, path: str, to: str) -> None:
        return None


class _RowsRoute:
    """Rows rendered as files, which a save replaces. Only in a folder of one
    file per row are they made, deleted and renamed.

    A save goes over the version the sender names, which the route checks
    under its write lock. It stores the rows, not the bytes, so the save
    answers with the file as its own transaction left it, which is the
    version and content the sender holds next.
    """

    def __init__(self, route: DbJsonRoute) -> None:
        self._route = route
        self.root_prefix = route.root_prefix

    def is_writable(self, path: str) -> bool:
        return self._route.is_writable(path)

    async def movable(self, path: str) -> bool:
        return False

    async def list(self, path: str) -> list[dict[str, Any]] | None:
        return await self._route.alist(path)

    async def list_tree(self, path: str, inline: InlineBudget) -> None:
        return None

    async def read(self, path: str) -> tuple[str, str] | None:
        return await self._route.aread_versioned(path)

    async def write(self, path: str, content: str, version: str | None) -> Saved:
        if version is None and self._route.is_fixed_path(path):
            raise LivefsError(Refusal.EXISTS, f"{path} already exists", path)
        with _refusing(path):
            stored = await self._route.awrite_versioned(path, content, version)
        final = stored.content
        if final is None:
            # The sender drops the file, as after an unlink.
            return Saved("", 0, path, stored.report, False, removed=True)
        return Saved(stored.version, len(final.encode()), path, stored.report, final == content)

    async def delete(self, path: str, version: str | None = None) -> Removed | None:
        with _refusing(path):
            if isinstance(self._route, DbJsonFolderRoute):
                stored = await self._route.adelete_versioned(path, version)
            else:
                # Its files are fixed, so it refuses every delete.
                stored = await self._route.adelete_versioned(path)
        return None if stored is None else Removed(stored.report)

    async def rename(self, path: str, to: str) -> str | None:
        if not isinstance(self._route, DbJsonFolderRoute):
            return None
        with _refusing(path):
            report = await self._route.arename_versioned(path, to)
        if report is None:
            raise not_found(path)
        return report


def adapt(route: Any) -> MountRoute:
    """The mount's view of a route the file tools' filesystem resolved."""
    if isinstance(route, DbJsonRoute):
        return _RowsRoute(route)
    return _StoreRoute(route)
