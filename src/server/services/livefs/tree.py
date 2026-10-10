"""Mount paths resolved onto the store routes the file tools already use.

The mount shows the user's files at paths of its own (``user/memory/...``,
``workflows/...``, ``workspaces/<id>/memory/...``), which the sandbox links in
where the file tools show the same files; ``POINTS`` declares both sides once.
Past threads (``workspaces/<id>/transcripts/...``, ``computer/threads.jsonl``)
are the exception: read-only renders the file tools reach only through the
links. Each request builds the routes the agent's filesystem gets, over a
stand-in sandbox that only answers path questions, so the mount and the tools
agree on what is readable, writable and valid by construction, and a path no
route owns has no files behind it to serve.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, NamedTuple

from ptc_agent.agent.backends import CompositeFilesystemBackend
from ptc_agent.agent.filesystem_routes import (
    build_filesystem_backend,
    resolve_identity_gates,
)
from ptc_agent.core.paths import USER_DATA_DIRS, SandboxLayout, WorkspaceLayout, logged_path
from ptc_agent.core.sandbox.livefs_mount import CallContext
from ptc_agent.core.sandbox.livefs_runtime.protocol import (
    INLINE_LISTING_MAX_BYTES,
    INLINE_TREE_MAX_BYTES,
    MAX_FILE_BYTES,
    MOUNT,
    Refusal,
    etag_version,
)
from src.observability.private_errors import failure
from src.server.database import workspace as workspace_db
from src.server.database.workspace_folders import is_top_level
from src.server.services import transcripts
from src.server.services.livefs.history import IndexRoute, TranscriptRoute
from src.server.services.livefs.routes import (
    InlineBudget,
    LivefsError,
    MountRoute,
    Saved,
    adapt,
    is_root,
    not_found,
)
from src.server.services.livefs.tokens import LivefsIdentity

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MountPoint:
    """A directory the mount serves, and where the sandbox links it in.

    A point with files is there only where the file tools' routes serve it,
    so the gates decide, never this table: without a store, memory is not.
    A point of past threads is always there, served by its own route.
    """

    name: str
    #: Where it is linked, under the computer root or a workspace folder.
    target: str
    #: One per workspace (``workspaces/<id>/<name>``) instead of per computer.
    per_workspace: bool = False
    #: Also linked in every workspace folder, where Bash and code run and
    #: the prompts' relative paths land.
    in_folders: bool = False
    #: The one file under ``target`` that is linked, when not all of it.
    only: str = ""
    #: The directories under ``target`` routes serve, when none serves it.
    tiers: tuple[str, ...] = ()
    #: The route of past threads, from (computer id, workspace id, layout).
    history: Callable[[str, str | None, WorkspaceLayout], MountRoute] | None = None


POINTS = (
    MountPoint(
        "user",
        SandboxLayout.USER_DIR,
        in_folders=True,
        tiers=tuple(
            directory.removeprefix(SandboxLayout.USER_DIR + "/")
            for directory in (
                SandboxLayout.MEMORY_USER_DIR,
                SandboxLayout.MEMO_USER_DIR,
                *USER_DATA_DIRS,
            )
        ),
    ),
    MountPoint("workflows", SandboxLayout.WORKFLOWS_DIR, in_folders=True),
    MountPoint(
        "computer",
        SandboxLayout.AGENTS_DIR,
        only=transcripts.INDEX,
        history=lambda computer_id, _, layout: IndexRoute(computer_id, layout.root),
    ),
    MountPoint("memory", WorkspaceLayout.MEMORY_DIR, per_workspace=True),
    MountPoint(
        "transcripts",
        WorkspaceLayout.TRANSCRIPTS_DIR,
        per_workspace=True,
        history=lambda _, workspace_id, layout: TranscriptRoute(str(workspace_id), layout),
    ),
)
_COMPUTER_POINTS = {p.name: p for p in POINTS if not p.per_workspace}
_WORKSPACE_POINTS = {p.name: p for p in POINTS if p.per_workspace}
_WORKSPACES = "workspaces"
# Longer than any path the sandbox can open.
_MAX_PATH_BYTES = 4096
# A NUL reaches Postgres, which refuses it as an outage would; the rest have
# no place in a name the tools show.
_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f]")


def _mounted(path: str) -> str:
    """The sandbox path of a mount path nothing links in: through the mount."""
    relative = path.strip("/")
    return f"{MOUNT}/{relative}" if relative else MOUNT


def _segments(path: str) -> tuple[str, ...]:
    if len(path.encode()) > _MAX_PATH_BYTES:
        # Named by its start: the refusal reaches the tool result.
        shown = _mounted(_CONTROL.sub("?", path[:64])) + "..."
        raise LivefsError(
            Refusal.INVALID, f"Path longer than {_MAX_PATH_BYTES} bytes: {shown}", shown
        )
    parts = tuple(p for p in path.strip("/").split("/") if p)
    if any(p in (".", "..") for p in parts) or _CONTROL.search(path):
        shown = _mounted(_CONTROL.sub("?", path))
        raise LivefsError(Refusal.INVALID, f"Invalid path: {shown}", shown)
    return parts


def _is_directory(path: str) -> LivefsError:
    return LivefsError(Refusal.IS_DIRECTORY, f"{path} is a directory", path)


def _log_failure(level: int, message: str, path: str, exc: Exception) -> None:
    fields, trace = failure(exc)
    fields = {"path": logged_path(path), **fields}
    # In the message too: the log format prints no extras, and without the
    # traceback the line would not name the error.
    details = " ".join(f"{k}={v}" for k, v in fields.items())
    logger.log(level, "%s: %s", message, details, extra=fields, exc_info=trace)


@contextmanager
def _answering(path: str) -> Iterator[None]:
    """Any failure but a refusal, as one the daemon answers with a retry."""
    try:
        yield
    except LivefsError:
        raise
    except Exception as exc:
        _log_failure(logging.ERROR, "livefs request failed", path, exc)
        raise LivefsError(
            Refusal.UNAVAILABLE, f"{path} could not be reached; retry", path
        ) from exc


class _PathOnlySandbox:
    """What the store routes ask of a sandbox: path shape, never files."""

    filesystem_config = None

    def __init__(self, root: str) -> None:
        self.computer_root = root

    def normalize_path(self, path: str) -> str:
        return path

    def virtualize_path(self, path: str) -> str:
        return path

    def validate_path(self, path: str) -> bool:
        return True


@dataclass(frozen=True)
class _Dir:
    """A directory the mount makes up from the table or the database."""

    path: str
    names: tuple[str, ...]


@dataclass(frozen=True)
class _Routed:
    """A path a route answers for."""

    route: MountRoute
    path: str

    @property
    def is_root(self) -> bool:
        return is_root(self.route.root_prefix, self.path)


class _Scope(NamedTuple):
    """Where points resolve: the computer root, or one workspace's folder."""

    workspace_id: str | None
    layout: WorkspaceLayout


class Listing(NamedTuple):
    entries: list[dict[str, Any]]
    #: New files can be created in it.
    writable: bool
    #: Made up from the layout and the computer's workspaces rather than
    #: served by a route, so it changes only when those do, which the host
    #: answers by running ``link`` again.
    structural: bool
    #: The directories below it the route listed with it, each whole, by
    #: path relative to it: ``{"entries": [...], "writable": bool}``.
    below: dict[str, dict[str, Any]]


def _carried(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop the content a listing may not carry."""
    inline = InlineBudget(INLINE_LISTING_MAX_BYTES)
    for entry in entries:
        if "content" in entry and not inline.take(entry["size"]):
            del entry["content"]
    return entries


def _link(mount_path: str, point: MountPoint, layout: WorkspaceLayout) -> tuple[str, str]:
    if point.only:
        return f"{mount_path}/{point.only}", layout.join(point.target, point.only)
    return mount_path, layout.join(point.target)


class LivefsTree:
    """One request's view of a computer's mount. Holds no state past it."""

    def __init__(
        self, identity: LivefsIdentity, store: Any, context: CallContext | None = None
    ) -> None:
        self._identity = identity
        self._store = store
        self._context = context or CallContext()
        self._layout = SandboxLayout.for_root(identity.root_dir)
        self._built: dict[_Scope, CompositeFilesystemBackend | None] = {}

    def _route_for(self, scope: _Scope, path: str) -> Any:
        # Built on first use, which past threads never make.
        if scope not in self._built:
            self._built[scope] = self._build(scope)
        files = self._built[scope]
        found = files.route_for(path) if files is not None else None
        # A route kept to the file tools is no point here, whatever the gates.
        return found if getattr(found, "mountable", True) else None

    def _build(self, scope: _Scope) -> CompositeFilesystemBackend | None:
        user_id = self._identity.user_id
        gates = resolve_identity_gates(
            store=self._store,
            user_id=user_id,
            workspace_id=scope.workspace_id,
            disable_subagents=True,
        )
        backend, _ = build_filesystem_backend(
            backend=_PathOnlySandbox(self._layout.root),
            gates=gates,
            store=self._store,
            user_id=user_id,
            workspace_id=scope.workspace_id,
            layout=scope.layout,
            call=self._context,
        )
        return backend if isinstance(backend, CompositeFilesystemBackend) else None

    def _computer(self) -> _Scope:
        # The call's workspace is where a new automation runs by default.
        return _Scope(self._context.workspace_id, self._layout.for_workspace(None))

    async def _workspace(self, workspace_id: str, path: str) -> _Scope:
        """A workspace of the user's on this computer, at its folder here.

        A workspace on another computer has no folder here to link or name,
        and separate computers are the isolation boundary, so it is not found.
        """
        workspace = await workspace_db.get_workspace_placement(workspace_id)
        if (
            workspace is None
            or workspace.get("user_id") != self._identity.user_id
            or str(workspace.get("computer_id")) != self._identity.computer_id
        ):
            raise not_found(path)
        layout = self._layout.for_workspace(workspace.get("dir_name"))
        return _Scope(str(workspace["workspace_id"]), layout)

    def _present(self, point: MountPoint, scope: _Scope) -> bool:
        if point.history is not None:
            return True
        target = scope.layout.join(point.target)
        return any(
            self._route_for(scope, path) is not None
            for path in (target, *(f"{target}/{tier}" for tier in point.tiers))
        )

    def _at(self, point: MountPoint, scope: _Scope, rest: tuple[str, ...]) -> _Dir | _Routed:
        path = scope.layout.join(point.target, *rest)
        if point.history is not None:
            route = point.history(self._identity.computer_id, scope.workspace_id, scope.layout)
            return _Routed(route, path)
        if point.tiers and not rest:
            tiers = tuple(
                t for t in point.tiers if self._route_for(scope, f"{path}/{t}") is not None
            )
            return _Dir(path, tiers)
        found = self._route_for(scope, path)
        if found is None:
            raise not_found(path)
        return _Routed(adapt(found), path)

    async def _resolve(self, parts: tuple[str, ...]) -> _Dir | _Routed:
        mounted = _mounted("/".join(parts))
        if not parts:
            computer = self._computer()
            names = [p.name for p in _COMPUTER_POINTS.values() if self._present(p, computer)]
            return _Dir(mounted, (*names, _WORKSPACES))
        head, rest = parts[0], parts[1:]
        if head != _WORKSPACES:
            if head not in _COMPUTER_POINTS:
                raise not_found(mounted)
            return self._at(_COMPUTER_POINTS[head], self._computer(), rest)
        if not rest:
            folders = await workspace_db.get_live_workspace_folders_for_computer(
                self._identity.computer_id
            )
            return _Dir(mounted, tuple(str(f["workspace_id"]) for f in folders))
        scope = await self._workspace(rest[0], mounted)
        if len(rest) == 1:
            names = tuple(p.name for p in _WORKSPACE_POINTS.values() if self._present(p, scope))
            return _Dir(mounted, names)
        if rest[1] not in _WORKSPACE_POINTS:
            raise not_found(mounted)
        return self._at(_WORKSPACE_POINTS[rest[1]], scope, rest[2:])

    async def _node(self, path: str) -> _Dir | _Routed:
        with _answering(_mounted(path)):
            return await self._resolve(_segments(path))

    async def _file(self, path: str) -> _Routed:
        node = await self._node(path)
        if isinstance(node, _Dir) or node.is_root:
            raise _is_directory(node.path)
        return node

    async def _missing(self, node: _Routed) -> LivefsError:
        """Why a path its route owns holds no file."""
        if await node.route.list(node.path) is not None:
            return _is_directory(node.path)
        return not_found(node.path)

    async def links(self) -> dict[str | None, list[tuple[str, str]]]:
        """Each mount path and the sandbox path the file tools show its files
        at, by the workspace whose folder holds it (None: the computer's own).
        Every live folder is a key, so the keys are the workspaces a link
        covers."""
        computer = self._computer()
        present = [p for p in _COMPUTER_POINTS.values() if self._present(p, computer)]
        links: dict[str | None, list[tuple[str, str]]] = {
            None: [_link(p.name, p, computer.layout) for p in present]
        }
        for folder in await workspace_db.get_live_workspace_folders_for_computer(
            self._identity.computer_id
        ):
            dir_name = folder.get("dir_name")
            if dir_name and not is_top_level(dir_name):
                # Staged mid-move: a link there would make the staging folder
                # the move treats as the content. It is linked once it lands.
                continue
            workspace_id = str(folder["workspace_id"])
            scope = _Scope(workspace_id, self._layout.for_workspace(dir_name))
            # A folder that owns the root reads the computer's own links.
            own = [
                _link(p.name, p, scope.layout)
                for p in present
                if p.in_folders and dir_name
            ]
            own += [
                _link(f"{_WORKSPACES}/{workspace_id}/{p.name}", p, scope.layout)
                for p in _WORKSPACE_POINTS.values()
                if self._present(p, scope)
            ]
            links[workspace_id] = own
        return links

    async def list(self, path: str) -> Listing:
        node = await self._node(path)
        if isinstance(node, _Dir):
            entries = [{"name": name, "type": "dir"} for name in node.names]
            return Listing(entries, False, True, {})
        route = node.route
        with _answering(node.path):
            tree = await route.list_tree(node.path, InlineBudget(INLINE_TREE_MAX_BYTES))
            if tree is None:
                listed = await route.list(node.path)
                if listed is None:
                    raise not_found(node.path)
                return Listing(_carried(listed), route.is_writable(node.path), False, {})
        below = {
            relative: {
                "entries": entries,
                "writable": route.is_writable(f"{node.path}/{relative}"),
            }
            for relative, entries in tree.items()
            if relative
        }
        return Listing(tree[""], route.is_writable(node.path), False, below)

    async def read(self, path: str) -> tuple[str, str, str]:
        """Content, version and the sandbox path the agent knows it by."""
        node = await self._file(path)
        with _answering(node.path):
            served = await node.route.read(node.path)
            if served is None:
                raise await self._missing(node)
        return (*served, node.path)

    async def write(
        self,
        path: str,
        data: bytes,
        *,
        if_match: str | None,
        if_none_match: str | None,
    ) -> Saved:
        """Replace a whole file on the precondition the caller saw: the
        version it read, or that there was none, so a save never lands over
        a change the caller did not see."""
        node = await self._file(path)
        with _answering(node.path):
            if len(data) > MAX_FILE_BYTES:
                raise LivefsError(
                    Refusal.TOO_LARGE,
                    f"{node.path}: files here hold at most {MAX_FILE_BYTES} bytes",
                    node.path,
                )
            try:
                content = data.decode("utf-8")
            except UnicodeDecodeError:
                raise LivefsError(
                    Refusal.INVALID, f"{node.path}: these files hold UTF-8 text", node.path
                ) from None
            if "\x00" in content:
                # Postgres would refuse it, and the save would answer as an
                # outage to retry.
                raise LivefsError(
                    Refusal.INVALID,
                    f"{node.path}: binary content (NUL bytes) cannot be saved here",
                    node.path,
                )
            if if_none_match == "*":
                version = None
            elif (version := etag_version(if_match)) is None:
                # An empty tag too: taken as no version, it would save only
                # where no file is.
                raise LivefsError(
                    Refusal.PRECONDITION_REQUIRED,
                    "Send If-Match or If-None-Match",
                    node.path,
                )
            return await node.route.write(node.path, content, version)

    async def delete(self, path: str) -> tuple[str, str | None]:
        """The path deleted, and what the delete changed, for a file that
        reports that."""
        node = await self._file(path)
        with _answering(node.path):
            removed = await node.route.delete(node.path)
            if removed is None:
                raise await self._missing(node)
        return node.path, removed.report

    async def rename(self, path: str, to: str) -> tuple[str, str, str | None]:
        """Move a file as its route moves one, else as write-then-delete, so
        a failure leaves the source.

        Directories are refused as ``is_directory``; the daemon answers that
        as a cross-device rename, which ``mv`` completes file by file. Also
        returns what the move changed, as ``write`` does.
        """
        source = await self._file(path)
        target = await self._file(to)
        if source.path == target.path:
            return source.path, target.path, None
        with _answering(source.path):
            if source.route.root_prefix == target.route.root_prefix:
                report = await source.route.rename(source.path, target.path)
                if report is not None:
                    return source.path, target.path, report
            if not await source.route.movable(source.path):
                # Before loading it, so an outage cannot answer as not found.
                raise LivefsError(
                    Refusal.READ_ONLY, f"{source.path} cannot be moved", source.path
                )
            served = await source.route.read(source.path)
            if served is None:
                raise await self._missing(source)
            # A rename replaces whatever is there, so it goes over the live version.
            current = await target.route.read(target.path)
            saved = await target.route.write(
                target.path, served[0], current[1] if current else None
            )
            try:
                # Over the version copied, so a save since is not deleted.
                await source.route.delete(source.path, served[1])
            except LivefsError as exc:
                if exc.code != Refusal.CHANGED:
                    raise
                await self._unmove(target, saved, current)
                raise LivefsError(
                    Refusal.CHANGED,
                    f"{source.path} changed while it was being moved, so it was not "
                    "moved. Move it again.",
                    source.path,
                ) from exc
        return source.path, target.path, saved.report

    async def _unmove(
        self, target: _Routed, saved: Saved, replaced: tuple[str, str] | None
    ) -> None:
        """Take back a refused move's copy: put back the file it replaced, or
        remove it. Over the version the copy left, so a save since stays."""
        left = None if saved.removed else saved.version
        try:
            if replaced is not None:
                await target.route.write(target.path, replaced[0], left)
            elif left is not None:
                await target.route.delete(target.path, left)
        except Exception as exc:
            _log_failure(logging.WARNING, "livefs move left its copy", target.path, exc)
