"""Authenticated workspace file CRUD routes (`/api/v1/workspaces/{id}/files*`)."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import shlex
import time
from contextlib import AsyncExitStack
from dataclasses import asdict
from datetime import UTC, datetime
from collections.abc import AsyncIterator, Sequence
from typing import Any

import anyio
from fastapi import APIRouter, Body, File, HTTPException, Query, Request, UploadFile
from pydantic import BaseModel, Field

from src.server.utils.api import CurrentUserId, require_workspace_owner
from src.server.services.persistence.transfer import scan_cap_bytes
from ptc_agent.agent.backends.db_json_route import DbJsonRoute
from ptc_agent.core.sandbox.runtime import STREAM_CHUNK_BYTES
from ptc_agent.core.paths import logged_path
from src.server.utils.uploads import read_capped
from src.utils.storage import is_storage_enabled
from src.server.utils.error_sanitization import (
    sandbox_failure_http,
    single_line,
)
from src.server.utils.http_headers import content_disposition
from fastapi.responses import Response, StreamingResponse

from src.server.database.home_workspace import is_unbound
from src.server.database.computer import get_computer
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.services.workspace_manager import WorkspaceManager
from src.server.services.persistence.file import FilePersistenceService
from src.server.services.persistence.download_link import (
    live_download_link,
    mirror_download_link,
)
from src.server.utils.secret_redactor import (
    get_redactor,
    get_vault_secrets_for_redaction,
)
from src.utils.mime import resolve_content_type

from .file_refs import (
    ResolveFileRefRequest,
    clean_candidates,
    clean_path,
    name_glob,
    resolve_file_ref,
    visible_paths,
)
from src.server.models.workspace import served_from_mirror

from src.server.services.workspace_file_containment import (
    UTF8_SNIFF_BYTES,
    FileChangedWhileRead,
    FileTooLargeToServe,
    contained_absolute_path,
    contained_sandbox_paths,
    contained_listing_path,
    contained_sandbox_path,
    is_within,
    normalize_requested_path,
    read_contained_sandbox_file,
    read_contained_sandbox_range,
)
from src.server.services.workspace_files import (
    HOME_NOT_ON_COMPUTER_REASON,
    acquire_sandbox_to_change,
    contained_target,
    not_running_for_upload,
    owned_workspace_on_computer,
    owner_layout,
    previous_dir_names_of,
    record_fs_bytes,
)

from ._shared import (
    held_bytes_budget,
    streamed_download_budget,
    DEFAULT_READ_LIMIT_LINES,
    TOO_LARGE_DETAIL,
    _is_text_content_type,
    _is_utf8,
    MAX_UPLOAD_BYTES,
    _CACHEABLE_IMAGE_TYPES,
    _decode_file_text,
    owner_work_dir,
    previous_roots_of,
    _is_always_hidden_path,
    _is_binary,
    _is_hidden_path,
    _is_system_path,
    _requested_hidden_ok,
    _requested_system_ok,
    _to_client_path,
    _present_virtual_file,
    _virtual_file,
    _virtual_files_in_scope,
    folder_unmoved,
    http_file_byte_range,
    http_file_bytes,
    http_file_text,
    may_be_utf8,
    requested_byte_range,
)
from .serve import warm_sandbox

logger = logging.getLogger(__name__)

# A tree the scan cannot read can fail thousands of paths. The backup
# route lists this many, and its counts stay exact.
_UNSAVED_LISTED = 100


router = APIRouter(prefix="/api/v1/workspaces", tags=["Workspace Files"])


async def _read_contained_target(
    sandbox: Any, path: str, work_dir: str, previous_dirs: Sequence[str] = ()
) -> tuple[str, bytes]:
    candidate = contained_absolute_path(path, work_dir, previous_dirs)
    if candidate is None or not sandbox.validate_path(candidate):
        raise HTTPException(status_code=404, detail="File not found")
    try:
        resolved = await read_contained_sandbox_file(
            sandbox, candidate, work_dir=work_dir
        )
    except FileTooLargeToServe:
        raise HTTPException(status_code=413, detail=TOO_LARGE_DETAIL) from None
    if resolved is None:
        raise HTTPException(status_code=404, detail="File not found")
    return resolved


async def _load_virtual_file(route: type[DbJsonRoute], path: str, user_id: str) -> str:
    try:
        text = await route.load(path, user_id)
    except Exception:
        logger.exception("virtual file read failed", extra={"path": logged_path(path)})
        raise HTTPException(status_code=500, detail=route.read_failure) from None
    if text is None:
        raise HTTPException(status_code=404, detail="File not found")
    return text


@router.get("/{workspace_id}/files")
async def list_workspace_files(
    workspace_id: str,
    x_user_id: CurrentUserId,
    path: str = Query(".", description="Directory to list (virtual or absolute)."),
    include_system: bool = Query(
        False,
        description="Include system and dependency directories (node_modules/, .venv/, etc.).",
    ),
    pattern: str = Query(
        "**/*", description="Glob pattern (evaluated in the sandbox)."
    ),
    wait_for_sandbox: bool = Query(
        False,
        description="If True, wait for sandbox to be ready. If False, return empty list if not ready.",
    ),
    auto_start: bool = Query(
        False,
        description="If True, auto-start a stopped workspace instead of returning DB-cached files.",
    ),
) -> dict[str, Any]:
    """List files in a workspace's sandbox, or from DB if stopped."""

    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=x_user_id)

    if is_unbound(workspace):
        return {"files": [], "sandbox_ready": False, "reason": HOME_NOT_ON_COMPUTER_REASON}

    work_dir = owner_work_dir(workspace)
    previous_dirs = previous_dir_names_of(workspace)
    # The directory to list, folded into this workspace's folder. A listing
    # root is the one place a client names a directory rather than a file, and
    # an unfolded one reaches a sibling's folder or the machine's shared /tmp.
    requested_dir = contained_listing_path(path, work_dir, previous_dirs)
    if requested_dir is None:
        raise HTTPException(status_code=404, detail="Not found")

    # DB fallback for stopped workspaces (unless auto_start requested)
    if not auto_start and served_from_mirror(workspace.get("status")):
        file_tree = await FilePersistenceService.get_file_tree(workspace_id)
        # Filter by path prefix if specified
        normalized_path = normalize_requested_path(path, work_dir, previous_dirs)
        if normalized_path:
            file_tree = [
                f
                for f in file_tree
                if f["path"].startswith(normalized_path + "/")
                or f["path"] == normalized_path
            ]
        allow_hidden = _requested_hidden_ok(path, work_dir, previous_dirs)
        files = [
            f["path"]
            for f in file_tree
            if not _is_always_hidden_path(f["path"])
            and (include_system or not _is_system_path(f["path"]))
            and (allow_hidden or not _is_hidden_path(f["path"]))
            # A row at a virtual path predates the backup skipping its folder;
            # a read answers from the DB, so only the splice may list one.
            and not _virtual_file(f["path"])
        ]
        files = await _with_virtual_files(files, normalized_path, workspace["user_id"])
        return {
            "workspace_id": workspace_id,
            "path": path,
            "files": files,
            "sandbox_ready": False,
            "source": "database",
        }

    async with acquire_sandbox_to_change(workspace_id, x_user_id) as (sandbox, workspace):
        # Fast path: return empty list if sandbox is still initializing and wait_for_sandbox=False
        # This allows CLI autocomplete to populate later without blocking startup
        if not wait_for_sandbox and not sandbox.is_ready():
            return {"files": [], "sandbox_ready": False}
        files = await _live_listing(sandbox, workspace, path, pattern, include_system)
    return {
        "workspace_id": workspace_id,
        "path": path,
        "files": files,
        "sandbox_ready": True,
    }


async def _live_listing(
    sandbox: Any,
    workspace: dict[str, Any],
    path: str,
    pattern: str,
    include_system: bool,
) -> list[str]:
    work_dir = owner_work_dir(workspace)
    previous_dirs = previous_dir_names_of(workspace)
    requested_dir = contained_listing_path(path, work_dir, previous_dirs)
    if requested_dir is None:
        raise HTTPException(status_code=404, detail="Not found")

    # Allow explicit listing of hidden internal paths (e.g. _internal/...).
    # No pre-flight health probe: ``aglob_files`` raises on an unreachable
    # sandbox rather than returning [], so the glob already reports the truth
    # and an extra round trip on every listing would buy nothing.
    allow_denied = _requested_hidden_ok(path, work_dir, previous_dirs)
    glob_root = f"{work_dir}/{requested_dir}" if requested_dir else work_dir
    # Canonicalise the root before walking it: a symlinked directory inside the
    # folder is how a contained request lists a sibling's files.
    glob_root = await contained_sandbox_path(sandbox, glob_root, work_dir=work_dir)
    if glob_root is None:
        raise HTTPException(status_code=404, detail="Not found")
    absolute_paths: list[str] = await sandbox.aglob_files(
        pattern, path=glob_root, allow_denied=allow_denied
    )

    allow_hidden = _requested_hidden_ok(path, work_dir, previous_dirs)

    files: list[str] = []
    for absolute_path in absolute_paths:
        if not is_within(work_dir, absolute_path):
            continue
        client_path = _to_client_path(sandbox, absolute_path, work_dir)

        # Always hide internal cache/bytecode/bootstrap artifacts.
        if _is_always_hidden_path(client_path):
            continue

        # Hide internal SDK/package directories unless explicitly requested.
        if not allow_hidden and _is_hidden_path(client_path):
            continue

        # Hide system directories unless explicitly requested or include_system=True.
        if (
            not include_system
            and _is_system_path(client_path)
            and not _requested_system_ok(path, work_dir, previous_dirs)
        ):
            continue

        files.append(client_path)

    requested_norm = normalize_requested_path(path, work_dir, previous_dirs)
    return await _with_virtual_files(files, requested_norm, workspace["user_id"])


async def _with_virtual_files(
    files: list[str], requested_norm: str, user_id: str
) -> list[str]:
    """Splice in the virtual DB-backed files (user profile, automations) the
    request scope covers.

    Their rows are the only copy: they don't exist on the sandbox FS, so
    aglob_files never returns them, and a backup skips their folder, so a
    stopped workspace's stored copy has none either.
    """
    for virtual_path in await _virtual_files_in_scope(requested_norm, user_id):
        if virtual_path not in files:
            files.append(virtual_path)
    return files


def _folded_refs(
    body: ResolveFileRefRequest, workspace: dict[str, Any]
) -> tuple[str, list[str], list[str]]:
    """The workspace's folder, and the reference and recent writes folded onto it."""
    work_dir = owner_work_dir(workspace)
    previous_dirs = previous_dir_names_of(workspace)
    previous_roots = previous_roots_of(workspace)
    candidates = clean_candidates(
        body.candidates, work_dir, previous_dirs, previous_roots=previous_roots
    )
    recent_writes = [
        p
        for p in (
            clean_path(w, work_dir, previous_dirs, previous_roots=previous_roots)
            for w in body.recent_writes
        )
        if p
    ]
    return work_dir, candidates, recent_writes


@router.post("/{workspace_id}/files/resolve")
async def resolve_workspace_file(
    workspace_id: str,
    x_user_id: CurrentUserId,
    body: ResolveFileRefRequest,
) -> dict[str, Any]:
    """Resolve a file reference to one workspace path, or say why it cannot.

    One name search over the live sandbox (or the persisted files of a stopped
    workspace) answers every reading of the reference at once, so the client
    never guesses from a listing that may predate the file.
    """
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=x_user_id)

    if is_unbound(workspace):
        return {"status": "unavailable", "reason": HOME_NOT_ON_COMPUTER_REASON, "matches": []}

    work_dir, candidates, recent_writes = _folded_refs(body, workspace)
    if not candidates:
        raise HTTPException(status_code=400, detail="A file reference is required")

    virtual = None
    for candidate in candidates:
        if await _present_virtual_file(candidate, x_user_id):
            virtual = candidate
            break
    if virtual:
        return {
            "status": "resolved",
            "path": virtual,
            "match": "exact",
            "matches": [virtual],
        }

    name = candidates[0].rsplit("/", 1)[-1]
    if served_from_mirror(workspace.get("status")):
        file_tree = await FilePersistenceService.get_file_tree(workspace_id)
        paths = [f["path"] for f in file_tree if f["path"].rsplit("/", 1)[-1] == name]
        source = "database"
    else:
        async with acquire_sandbox_to_change(workspace_id, x_user_id) as (sandbox, workspace):
            if not sandbox.is_ready():
                return {
                    "status": "unavailable",
                    "reason": "sandbox_starting",
                    "matches": [],
                }
            # The acquisition may have moved the folder, so the reference folds
            # again against the row it left.
            work_dir, refolded, recent_writes = _folded_refs(body, workspace)
            candidates = refolded or candidates
            # The search runs over this workspace's folder, canonicalised the way a
            # listing's root is: the handle's own root is the computer, which holds
            # every sibling's namesakes.
            glob_root = await contained_sandbox_path(sandbox, work_dir, work_dir=work_dir)
            if glob_root is None:
                raise HTTPException(status_code=404, detail="Not found")
            absolute_paths: list[str] = await sandbox.aglob_files(
                name_glob(name), path=glob_root
            )
        paths = [
            _to_client_path(sandbox, p, work_dir)
            for p in absolute_paths
            if is_within(work_dir, p)
        ]
        source = "sandbox"

    result = resolve_file_ref(
        candidates, visible_paths(paths, candidates), recent_writes
    )
    return {**result, "source": source}


@router.get("/{workspace_id}/files/read")
async def read_workspace_file(
    workspace_id: str,
    x_user_id: CurrentUserId,
    path: str = Query(..., description="File path (virtual or absolute)."),
    offset: int = Query(0, ge=0, description="Line offset (0-based)."),
    limit: int = Query(
        DEFAULT_READ_LIMIT_LINES,
        ge=1,
        le=DEFAULT_READ_LIMIT_LINES,
        description="Max lines.",
    ),
    unlimited: bool = Query(
        False,
        description="Return the full file content without line-range pagination.",
    ),
) -> dict[str, Any]:
    """Read a file from the workspace's sandbox, or from DB if stopped."""

    workspace = await owned_workspace_on_computer(workspace_id, x_user_id)

    # Virtual DB-backed JSON files (user profile, automations) are served from
    # the DB, independent of sandbox state. Works whether the workspace is
    # running, stopped, or never started.
    work_dir = owner_work_dir(workspace)
    previous_dirs = previous_dir_names_of(workspace)
    normalized_for_virtual = normalize_requested_path(path, work_dir, previous_dirs)
    virtual = _virtual_file(normalized_for_virtual)
    if virtual:
        text_content = await _load_virtual_file(
            virtual, normalized_for_virtual, x_user_id
        )
        if unlimited:
            content = text_content
            truncated = False
        else:
            lines = text_content.splitlines()
            content = "\n".join(lines[offset : offset + limit])
            truncated = len(lines) > offset + limit
        return {
            "workspace_id": workspace_id,
            "path": normalized_for_virtual,
            "offset": offset,
            "limit": limit,
            "content": content,
            "mime": "application/json",
            "truncated": truncated,
            "source": virtual.source,
        }

    # DB fallback for stopped workspaces
    if served_from_mirror(workspace.get("status")):
        normalized_path = normalize_requested_path(path, work_dir, previous_dirs)
        if not normalized_path:
            raise HTTPException(status_code=400, detail="File path is required")

        # Parallel: fetch vault secrets + file content in one round-trip window
        vault_secrets, file_record = await asyncio.gather(
            get_vault_secrets_for_redaction(workspace["user_id"]),
            FilePersistenceService.get_file_content(workspace_id, normalized_path),
        )
        if not file_record:
            raise HTTPException(status_code=404, detail="File not found")

        if file_record.get("is_binary"):
            raise HTTPException(
                status_code=415,
                detail="Cannot read binary file as text. Use GET /files/download instead.",
            )

        # Never `.get("content_text", "")` — the key exists with a NULL value on
        # a blob-backed row, so the default never fires and redact() would be
        # handed None.
        text_content = await http_file_text(file_record, user_id=workspace["user_id"])
        text_content = get_redactor().redact(text_content, vault_secrets=vault_secrets)
        if unlimited:
            content = text_content
            truncated = False
        else:
            lines = text_content.splitlines()
            content = "\n".join(lines[offset : offset + limit])
            truncated = len(lines) > offset + limit
        mime = file_record.get("mime_type") or "text/plain"

        return {
            "workspace_id": workspace_id,
            "path": normalized_path,
            "offset": offset,
            "limit": limit,
            "content": content,
            "mime": mime,
            "truncated": truncated,
            "source": "database",
        }

    async with acquire_sandbox_to_change(workspace_id, x_user_id) as (sandbox, workspace):
        work_dir = owner_work_dir(workspace)
        previous_dirs = previous_dir_names_of(workspace)

        normalized, raw_bytes = await _read_contained_target(
            sandbox, path, work_dir, previous_dirs
        )

    # Check for known binary extensions
    if _is_binary(normalized):
        raise HTTPException(
            status_code=415,
            detail="Cannot read binary file as text. Use GET /files/download instead.",
        )

    decoded = _decode_file_text(raw_bytes)
    if decoded is None:
        raise HTTPException(
            status_code=415,
            detail="File appears to be binary and cannot be read as text. Use GET /files/download instead.",
        )
    text_content = decoded

    vault_secrets = await get_vault_secrets_for_redaction(workspace["user_id"])
    text_content = get_redactor().redact(text_content, vault_secrets=vault_secrets)

    # Apply line range (skip when unlimited=True for edit mode)
    if unlimited:
        content = text_content
        truncated = False
    else:
        lines = text_content.splitlines()
        content = "\n".join(lines[offset : offset + limit])
        truncated = len(lines) > offset + limit

    client_path = _to_client_path(sandbox, normalized, work_dir)
    if _is_always_hidden_path(client_path):
        raise HTTPException(status_code=404, detail="File not found")

    mime = resolve_content_type(client_path, default="text/plain")

    record_fs_bytes("read", len(raw_bytes))

    return {
        "workspace_id": workspace_id,
        "path": client_path,
        "offset": offset,
        "limit": limit,
        "content": content,
        "mime": mime,
        "truncated": truncated,
    }


MAX_WRITE_BYTES = 10 * 1024 * 1024  # 10MB text write limit


class WriteFileRequest(BaseModel):
    content: str = Field(..., description="File content to write.")


@router.put("/{workspace_id}/files/write")
async def write_workspace_file(
    workspace_id: str,
    x_user_id: CurrentUserId,
    path: str = Query(..., description="File path (virtual or absolute)."),
    body: WriteFileRequest = Body(...),
) -> dict[str, Any]:
    """Write text content to a file in the workspace's sandbox."""

    workspace = await owned_workspace_on_computer(workspace_id, x_user_id)

    if served_from_mirror(workspace.get("status")):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot write files — workspace is {workspace.get('status')}. Wait for it to be running.",
        )

    # Virtual DB-backed files are read-only through this API. Writes happen
    # via their own UI (dashboard widgets, the Automations page) or the agent's
    # CompositeFilesystemBackend route (UserDataBackend, AutomationsBackend),
    # both of which apply schema validation and version checks that this
    # generic write endpoint cannot enforce safely.
    work_dir = owner_work_dir(workspace)
    previous_dirs = previous_dir_names_of(workspace)
    virtual = _virtual_file(normalize_requested_path(path, work_dir, previous_dirs))
    if virtual:
        raise HTTPException(status_code=400, detail=virtual.read_only)

    content_bytes = body.content.encode("utf-8")
    if len(content_bytes) > MAX_WRITE_BYTES:
        raise HTTPException(status_code=413, detail="File content too large (max 10MB)")

    async with acquire_sandbox_to_change(workspace_id, x_user_id) as (sandbox, workspace):
        work_dir = owner_work_dir(workspace)
        previous_dirs = previous_dir_names_of(workspace)

        normalized = await contained_target(sandbox, path, work_dir, previous_dirs)

        # ``awrite_file_text`` takes this path's write lock for the write itself.
        ok = await sandbox.awrite_file_text(normalized, body.content)
    if not ok:
        raise HTTPException(status_code=500, detail="Write failed")

    # Invalidate agent.md cache when user edits agent.md via UI
    client_path = _to_client_path(sandbox, normalized, work_dir)
    if client_path == "agent.md":
        try:
            manager = WorkspaceManager.get_instance()
            # The cache is keyed by machine; the project resolves to one only
            # while this worker is serving it, which is the only time a stamp
            # has a session to land on.
            computer_id = manager._live_session_computer(workspace_id)
            session = manager._cached_session(computer_id) if computer_id else None
            if session:
                # Stamp the writer too, so the agent's runtime-context baseline
                # attributes the next diff to the user rather than to whoever
                # wrote agent.md last from inside a turn.
                session.note_agent_md_write(
                    {
                        "writer": "user",
                        "at": datetime.now(UTC).isoformat(),
                    }
                )
        except Exception:
            pass

    record_fs_bytes("write", len(content_bytes))

    return {
        "workspace_id": workspace_id,
        "path": client_path,
        "size": len(content_bytes),
    }


# How long a large download waits for a stream slot before answering 503.
_STREAM_SLOT_WAIT_S = 30
# How long the sandbox has to produce a stream's first chunk.
_STREAM_OPEN_TIMEOUT_S = 60
# How often a large download counts as use of its computer while it streams.
_STREAM_KEEP_ALIVE_S = 60


class _StreamedDownload(StreamingResponse):
    """A download streamed from the sandbox that keeps ``held`` open until sent.

    ``held`` owns the upstream stream, its slot in the stream budget and the
    hold on the workspace's folder, so all three last exactly as long as the
    client is reading, and a client that disconnects closes the upstream read
    with them.
    """

    def __init__(
        self, body: AsyncIterator[bytes], *, held: AsyncExitStack, **kwargs: Any
    ) -> None:
        super().__init__(body, **kwargs)
        self._held = held

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            # Shielded: this close releases the stream slot, and a cancelled
            # one would hold it for the life of the worker.
            with anyio.CancelScope(shield=True):
                await self._held.aclose()


class _FileChangedWhileSending(Exception):
    """The file grew or shrank after its size went out as Content-Length."""


async def _kept_alive(
    body: AsyncIterator[bytes], workspace_id: str, sandbox: Any
) -> AsyncIterator[bytes]:
    """Yield ``body``, counting it as use of the computer about once a minute.

    A download is no turn and, once open, makes no provider call, so neither
    the idle sweep nor E2B's deadline would see it, and a long one could be
    stopped mid-transfer.
    """
    due = 0.0
    async for chunk in body:
        now = time.monotonic()
        if now >= due:
            due = now + _STREAM_KEEP_ALIVE_S
            try:
                await WorkspaceManager.get_instance().keep_sandbox_alive(
                    workspace_id, sandbox.sandbox_id, runtime=sandbox.runtime
                )
            except Exception as e:
                logger.debug("Download did not count as activity: %s", e)
        yield chunk


async def _exactly(body: AsyncIterator[bytes], size: int) -> AsyncIterator[bytes]:
    """Yield ``body`` only while it matches ``size``.

    A file an agent is still writing can outgrow the size admitted for it.
    Stopping at that size would hand the client a silent prefix, so the send
    fails instead and the client sees a broken download.
    """
    sent = 0
    async for chunk in body:
        sent += len(chunk)
        if sent > size:
            raise _FileChangedWhileSending(f"grew past {size} bytes")
        yield chunk
    if sent != size:
        raise _FileChangedWhileSending(f"ended at {sent} of {size} bytes")


def _build_download_response(
    content: bytes,
    filename: str,
    mime: str,
    request: Request,
    disposition: str = "inline",
) -> Response:
    """Build a download response with caching headers for image types."""
    etag = hashlib.md5(content).hexdigest()
    headers: dict[str, str] = {
        "Content-Disposition": content_disposition(filename, disposition=disposition),
        "ETag": f'"{etag}"',
    }
    if mime in _CACHEABLE_IMAGE_TYPES:
        headers["Cache-Control"] = "private, max-age=300"
    else:
        headers["Cache-Control"] = "private, no-cache"

    # Return 304 if client already has this version
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match.strip('" ') == etag:
        return Response(status_code=304, headers=headers)

    return Response(content=content, media_type=mime, headers=headers)


# The most one ranged answer carries, so a range of any width holds a bounded
# slice in memory and a client loops on a short answer. It also bounds a live
# slice's exec output, so it must not pass EXEC_READ_MAX_BYTES, the most one
# exec read may carry.
RANGE_MAX_BYTES = 8 * 1024 * 1024


def _range_length(first: int, last: int) -> int:
    """The most a range is answered with; the file's end can cut it shorter."""
    return min(last - first + 1, RANGE_MAX_BYTES)


def _starts_as_text(head: bytes, size: int) -> bool:
    """Whether a file whose first bytes are ``head`` might be text, which goes
    whole so that it is redacted like any other text answer."""
    return may_be_utf8(head, whole=len(head) >= size)


def _range_response(
    content: bytes,
    first: int,
    size: int,
    etag: str,
    filename: str,
    mime: str,
    disposition: str,
) -> Response:
    """A 206 carrying ``content`` from byte ``first`` of a binary file.

    Unredacted, like a streamed download and the signed link: redaction needs
    the whole body, so only a file whose type and first bytes already rule out
    text takes a range, and its whole answer would not be redacted either.
    """
    return Response(
        content=content,
        status_code=206,
        media_type=mime,
        headers={
            "Content-Range": f"bytes {first}-{first + len(content) - 1}/{size}",
            "Accept-Ranges": "bytes",
            "ETag": f'"{etag}"',
            # The reader holds what it fetched. A slice the browser stored would
            # only be revalidated against a route that answers every range anew.
            # Each slice is held to the first's exact ETag; re-encoding weakens it.
            "Cache-Control": "private, no-store, no-transform",
            "Content-Disposition": content_disposition(
                filename, disposition=disposition
            ),
        },
    )


def _unsatisfiable(size: int) -> HTTPException:
    return HTTPException(
        status_code=416,
        detail="Requested range not satisfiable",
        headers={"Content-Range": f"bytes */{size}"},
    )


async def _live_range_response(
    sandbox: Any,
    candidate: str,
    work_dir: str,
    byte_range: tuple[int, int],
    disposition: str,
) -> Response | None:
    """A range of a live file, read in the exec that checks its containment.

    None for a file that goes whole: one that may be text (a binary name linked
    to a text file, or text under a binary type), and one the sandbox gave no
    version for, whose size alone cannot tell two versions apart. A 416
    carries no validator, so it is decided on the size alone.
    """
    first, last = byte_range
    try:
        sliced = await read_contained_sandbox_range(
            sandbox,
            candidate,
            work_dir=work_dir,
            offset=first,
            length=_range_length(first, last),
        )
    except FileChangedWhileRead:
        # The file panel tells this apart from an unreachable sandbox by code.
        raise HTTPException(
            status_code=503,
            detail={
                "code": "file_changed",
                "message": "File changed while it was read",
            },
            headers={"Retry-After": "1"},
        ) from None
    if sliced is None:
        raise HTTPException(status_code=404, detail="File not found")
    client_path = _to_client_path(sandbox, sliced.canonical, work_dir)
    if _is_always_hidden_path(client_path):
        raise HTTPException(status_code=404, detail="File not found")
    filename = client_path.split("/")[-1] if client_path else "download"
    mime = resolve_content_type(filename)
    if _is_text_content_type(mime):
        return None
    if sliced.content is None:
        raise _unsatisfiable(sliced.size)
    if sliced.version is None or _starts_as_text(sliced.head, sliced.size):
        return None
    record_fs_bytes("download", len(sliced.content))
    return _range_response(
        sliced.content,
        first,
        sliced.size,
        f"{sliced.size}-{sliced.version}",
        filename,
        mime,
        disposition,
    )


async def _warm_range_response(
    workspace: dict[str, Any],
    workspace_id: str,
    candidate: str,
    work_dir: str,
    byte_range: tuple[int, int],
    disposition: str,
) -> tuple[bool, Response | None]:
    """:func:`_live_range_response` through the session this worker already
    holds, without a folder hold, as ``(settled, answer)``.

    An acquisition per slice would hold the folder nearly the whole time a
    reader scrolls, which keeps a rename's settle from ever moving it. So, like
    the URL-only routes, the read goes unheld and its answer stands only if the
    folder ``workspace`` names was still this workspace's afterwards. Without a
    warm session, a sandbox that answers, or that folder, ``settled`` is False
    and the request goes through the acquisition.
    """
    sandbox = warm_sandbox(workspace, workspace_id)
    if sandbox is None or not sandbox.validate_path(candidate):
        return False, None
    try:
        answer = await _live_range_response(
            sandbox, candidate, work_dir, byte_range, disposition
        )
    except RuntimeError as e:
        logger.info(
            f"Warm ranged read failed for workspace {workspace_id}; "
            f"acquiring instead: {single_line(str(e))}"
        )
        return False, None
    except HTTPException as e:
        # A workspace moving to another computer keeps its folder's name while
        # the folder is set aside, so a file missing here may only be away for
        # the move, which the acquisition answers as a 503 to retry.
        if e.status_code != 404 and await folder_unmoved(
            workspace_id, workspace.get("dir_name")
        ):
            raise
        return False, None
    if not await folder_unmoved(workspace_id, workspace.get("dir_name")):
        return False, None
    return True, answer


async def _stored_range_response(
    file_record: dict[str, Any],
    byte_range: tuple[int, int],
    filename: str,
    mime: str,
    disposition: str,
    *,
    user_id: str,
) -> Response | None:
    """A range of a mirrored file, reading only that slice and the file's
    first bytes from storage.

    None for a row that goes whole: one stored inline or without a digest to
    version it, and one that may be text.
    """
    etag = file_record.get("content_hash") or file_record.get("blob_sha256")
    if not etag:
        return None
    first, last = byte_range
    length = _range_length(first, last)
    if first == 0 and length >= UTF8_SNIFF_BYTES:
        read = head_read = await http_file_byte_range(
            file_record, 0, length, user_id=user_id
        )
    else:
        head_read, read = await asyncio.gather(
            http_file_byte_range(file_record, 0, UTF8_SNIFF_BYTES, user_id=user_id),
            http_file_byte_range(file_record, first, length, user_id=user_id),
        )
    if read is None or head_read is None:
        return None
    content, size = read
    if not content:
        raise _unsatisfiable(size)
    if _starts_as_text(head_read[0][:UTF8_SNIFF_BYTES], size):
        return None
    return _range_response(content, first, size, etag, filename, mime, disposition)


@router.get("/{workspace_id}/files/download")
async def download_workspace_file(
    workspace_id: str,
    x_user_id: CurrentUserId,
    request: Request,
    path: str = Query(..., description="File path (virtual or absolute)."),
    attachment: bool = Query(
        False, description="Ask the browser to save the file rather than show it."
    ),
) -> Response:
    """Download raw bytes from the workspace's sandbox, or from DB if stopped."""
    disposition = "attachment" if attachment else "inline"
    byte_range = requested_byte_range(request.headers)

    workspace = await owned_workspace_on_computer(workspace_id, x_user_id)

    work_dir = owner_work_dir(workspace)
    previous_dirs = previous_dir_names_of(workspace)

    # Virtual files come from the DB, as in /files/read: on the sandbox their
    # folder links out to the file mount, and the stored copy has no row for them.
    virtual_path = normalize_requested_path(path, work_dir, previous_dirs)
    virtual = _virtual_file(virtual_path)
    if virtual:
        text = await _load_virtual_file(virtual, virtual_path, x_user_id)
        filename = virtual_path.rsplit("/", 1)[-1]
        return _build_download_response(
            text.encode(), filename, "application/json", request, disposition
        )

    # DB fallback for stopped workspaces
    if served_from_mirror(workspace.get("status")):
        normalized_path = normalize_requested_path(path, work_dir, previous_dirs)
        if not normalized_path:
            raise HTTPException(status_code=400, detail="File path is required")

        if byte_range is None:
            # Parallel: fetch vault secrets + file content in one round-trip window
            vault_secrets, file_record = await asyncio.gather(
                get_vault_secrets_for_redaction(workspace["user_id"]),
                FilePersistenceService.get_file_content(workspace_id, normalized_path),
            )
        else:
            # A ranged answer is never redacted, so the secrets wait for the
            # row to say whether the range applies.
            file_record = await FilePersistenceService.get_file_content(
                workspace_id, normalized_path
            )
        if not file_record:
            raise HTTPException(status_code=404, detail="File not found")

        filename = file_record.get("file_name", "download")
        mime = file_record.get("mime_type") or "application/octet-stream"

        if byte_range is not None:
            if not _is_text_content_type(mime):
                ranged = await _stored_range_response(
                    file_record,
                    byte_range,
                    filename,
                    mime,
                    disposition,
                    user_id=workspace["user_id"],
                )
                if ranged is not None:
                    return ranged
            vault_secrets = await get_vault_secrets_for_redaction(workspace["user_id"])

        content = await http_file_bytes(file_record, user_id=workspace["user_id"])

        if _is_text_content_type(mime) or _is_utf8(content):
            content = get_redactor().redact_bytes(content, vault_secrets=vault_secrets)

        return _build_download_response(content, filename, mime, request, disposition)

    if byte_range is not None:
        candidate = contained_absolute_path(path, work_dir, previous_dirs)
        if candidate is not None and not _is_text_content_type(
            resolve_content_type(candidate.rsplit("/", 1)[-1])
        ):
            settled, ranged = await _warm_range_response(
                workspace, workspace_id, candidate, work_dir, byte_range, disposition
            )
            if ranged is not None:
                return ranged
            if settled:
                # The file goes whole, which another ranged read would only
                # find again.
                byte_range = None

    async with AsyncExitStack() as folder:
        # A streamed file is read by path chunk after chunk, so its hold goes
        # with the response; a settle moving the folder between two chunks
        # would end the download.
        sandbox, workspace = await folder.enter_async_context(
            acquire_sandbox_to_change(workspace_id, x_user_id)
        )
        work_dir = owner_work_dir(workspace)
        previous_dirs = previous_dir_names_of(workspace)

        candidate = contained_absolute_path(path, work_dir, previous_dirs)
        if candidate is None or not sandbox.validate_path(candidate):
            raise HTTPException(status_code=404, detail="File not found")
        if byte_range is not None and not _is_text_content_type(
            resolve_content_type(candidate.rsplit("/", 1)[-1])
        ):
            ranged = await _live_range_response(
                sandbox, candidate, work_dir, byte_range, disposition
            )
            if ranged is not None:
                return ranged
        try:
            resolved = await read_contained_sandbox_file(
                sandbox, candidate, work_dir=work_dir
            )
            too_large = None
        except FileTooLargeToServe as e:
            resolved, too_large = (e.canonical, b""), e
        if resolved is None:
            raise HTTPException(status_code=404, detail="File not found")
        normalized, content = resolved

        client_path = _to_client_path(sandbox, normalized, work_dir)
        if _is_always_hidden_path(client_path):
            raise HTTPException(status_code=404, detail="File not found")

        filename = client_path.split("/")[-1] if client_path else "download"
        mime = resolve_content_type(filename)

        if too_large is not None:
            return await _stream_large_file(
                workspace_id, sandbox, too_large, filename, mime, disposition, folder=folder
            )

        if _is_text_content_type(mime) or _is_utf8(content):
            vault_secrets = await get_vault_secrets_for_redaction(workspace["user_id"])
            content = get_redactor().redact_bytes(content, vault_secrets=vault_secrets)

        record_fs_bytes("download", len(content))

        return _build_download_response(content, filename, mime, request, disposition)


async def _stream_large_file(
    workspace_id: str,
    sandbox: Any,
    too_large: FileTooLargeToServe,
    filename: str,
    mime: str,
    disposition: str,
    *,
    folder: AsyncExitStack,
) -> Response:
    """Stream a file past the exec read's limit straight from the sandbox.

    Any file the store cannot carry lands here: every file without a store,
    one past what a relay export can hold, or one whose export failed.
    Streaming keeps a worker's memory flat at any size. Like the signed link,
    the body skips secret redaction: it is the owner's own file, and
    redaction needs it whole. ``folder`` holds the workspace's folder, and is
    released last, after the upstream read closes.
    """
    async with AsyncExitStack() as held:
        await held.enter_async_context(folder.pop_all())
        try:
            async with asyncio.timeout(_STREAM_SLOT_WAIT_S):
                await held.enter_async_context(
                    streamed_download_budget().hold(STREAM_CHUNK_BYTES)
                )
        except TimeoutError:
            # Slow readers can hold every slot for as long as they like, so a
            # queued download gives up rather than hang behind them.
            raise HTTPException(
                status_code=503,
                detail="Too many downloads in progress; try again shortly",
                headers={"Retry-After": str(_STREAM_SLOT_WAIT_S)},
            ) from None
        try:
            # Short, apart from the provider's hour-long read: a sandbox that
            # accepts the request and never answers would hold a slot for it.
            async with asyncio.timeout(_STREAM_OPEN_TIMEOUT_S):
                stream = await sandbox.astream_file_bytes(too_large.canonical)
        except TimeoutError:
            raise HTTPException(
                status_code=504, detail="The sandbox did not start sending the file"
            ) from None
        if stream is None:
            raise HTTPException(status_code=404, detail="File not found")
        held.push_async_callback(stream.aclose)
        record_fs_bytes("download", too_large.size)
        return _StreamedDownload(
            _kept_alive(_exactly(stream, too_large.size), workspace_id, sandbox),
            held=held.pop_all(),
            media_type=mime,
            headers={
                "Content-Disposition": content_disposition(
                    filename, disposition=disposition
                ),
                "Content-Length": str(too_large.size),
                "Cache-Control": "private, no-cache",
            },
        )


@router.get("/{workspace_id}/files/download-url")
async def workspace_file_download_url(
    workspace_id: str,
    x_user_id: CurrentUserId,
    path: str = Query(..., description="File path (virtual or absolute)."),
) -> dict[str, str | None]:
    """A short-lived store link for saving a file, or ``url: null`` to use /files/download.

    A bearer token cannot ride a browser navigation, so the download is two
    steps: this owner-checked call, then a plain GET the browser streams to
    disk. The link skips secret redaction: it is the owner's own file, and
    redacting would mean carrying the bytes through this process.
    """
    workspace = await owned_workspace_on_computer(workspace_id, x_user_id)

    layout = owner_layout(workspace)
    work_dir = layout.workspace
    previous_dirs = previous_dir_names_of(workspace)

    # A virtual file is rendered from its rows when downloaded, so there is no
    # stored object to link to.
    if _virtual_file(normalize_requested_path(path, work_dir, previous_dirs)):
        return {"url": None}

    if served_from_mirror(workspace.get("status")):
        normalized_path = normalize_requested_path(path, work_dir, previous_dirs)
        if not normalized_path:
            raise HTTPException(status_code=400, detail="File path is required")
        if _is_always_hidden_path(normalized_path):
            raise HTTPException(status_code=404, detail="File not found")
        return {"url": await mirror_download_link(workspace, normalized_path)}

    # Held through the export too, which reads the file by path.
    async with acquire_sandbox_to_change(workspace_id, x_user_id) as (sandbox, workspace):
        layout = owner_layout(workspace)
        work_dir = layout.workspace
        previous_dirs = previous_dir_names_of(workspace)
        try:
            canonical = await contained_target(sandbox, path, work_dir, previous_dirs)
        except HTTPException:
            # This check is the project folder's; reads also admit shared tiers
            # outside it. /files/download applies the read policy and refuses the
            # rest itself, so it decides, and no answer here says which it was.
            return {"url": None}
        client_path = _to_client_path(sandbox, canonical, work_dir)
        if _is_always_hidden_path(client_path):
            raise HTTPException(status_code=404, detail="File not found")
        rel_path = canonical[len(work_dir.rstrip("/")) + 1 :]
        try:
            url = await live_download_link(workspace, sandbox, rel_path, layout=layout)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="File not found") from None
    return {"url": url}


@router.post("/{workspace_id}/files/upload")
async def upload_workspace_file(
    workspace_id: str,
    x_user_id: CurrentUserId,
    path: str | None = Query(
        None,
        description="Destination path (virtual or absolute). Defaults to filename.",
    ),
    file: UploadFile = File(...),
) -> dict[str, Any]:
    """Upload a file to the workspace's live sandbox."""

    workspace = await owned_workspace_on_computer(workspace_id, x_user_id)

    if served_from_mirror(workspace.get("status")):
        raise not_running_for_upload(workspace)

    async with acquire_sandbox_to_change(workspace_id, x_user_id) as (sandbox, workspace):
        work_dir = owner_work_dir(workspace)
        previous_dirs = previous_dir_names_of(workspace)

        dest = path or file.filename
        if not dest:
            raise HTTPException(status_code=400, detail="Destination path is required")
        # The same refusal as write: an upload would land a sandbox file the
        # virtual route shadows, so it would read as saved and change nothing.
        virtual = _virtual_file(normalize_requested_path(dest, work_dir, previous_dirs))
        if virtual:
            raise HTTPException(status_code=400, detail=virtual.read_only)

        normalized = await contained_target(sandbox, dest, work_dir, previous_dirs)

        # Accept only what the next backup could actually store. This route
        # buffers the body, so its own ceiling applies too, and whichever is
        # tighter wins; taking the relay ceiling alone would accept an upload that
        # a deployment writing bytes inline then drops on the next sync.
        scan_cap = scan_cap_bytes(sandbox, blobs_on=is_storage_enabled())
        upload_cap = (
            MAX_UPLOAD_BYTES if scan_cap is None else min(MAX_UPLOAD_BYTES, scan_cap)
        )
        if file.size is not None and file.size > upload_cap:
            await read_capped(file, upload_cap)  # raises the 413 before queueing

        # The cap bounds one request; the budget bounds how many this worker
        # holds at once, the way the relay bounds its own.
        async with held_bytes_budget().hold(file.size):
            content = await read_capped(file, upload_cap)
            # ``aupload_file_bytes`` takes this path's write lock for the write itself.
            ok = await sandbox.aupload_file_bytes(normalized, content)
    if not ok:
        raise HTTPException(status_code=500, detail="Upload failed")

    client_path = _to_client_path(sandbox, normalized, work_dir)
    record_fs_bytes("upload", len(content))
    return {
        "workspace_id": workspace_id,
        "path": client_path,
        "size": len(content),
        "filename": file.filename,
    }


async def _computer_backs_up_files(workspace: dict[str, Any] | None) -> bool:
    """The switch of the computer the workspace is on; on where it has none."""
    computer_id = (workspace or {}).get("computer_id")
    if not computer_id:
        return True
    computer = await get_computer(str(computer_id))
    return (computer or {}).get("files_backup", True) is not False


_RESTORE_OFFER_STANDS = (
    "This workspace's folder is missing on its computer. Restore it "
    "from the backup or keep it as it is first."
)


@router.post("/{workspace_id}/files/backup")
async def backup_workspace_files(
    workspace_id: str,
    x_user_id: CurrentUserId,
) -> dict[str, Any]:
    """Backup workspace files from sandbox to DB for offline access."""

    workspace = await owned_workspace_on_computer(workspace_id, x_user_id)

    if served_from_mirror(workspace.get("status")):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot backup files — workspace is {workspace.get('status')}.",
        )
    if workspace.get("files_restore_offered_at"):
        raise HTTPException(status_code=409, detail=_RESTORE_OFFER_STANDS)
    if not await _computer_backs_up_files(workspace):
        raise HTTPException(
            status_code=409, detail="File backup is off for this computer."
        )

    # Held like a file change: a folder moved mid-scan reads as missing, and
    # the sync reports that as a clean pass.
    async with acquire_sandbox_to_change(workspace_id, x_user_id) as (sandbox, workspace):
        try:
            result = await FilePersistenceService.sync_to_db(
                workspace_id, sandbox, layout=owner_layout(workspace)
            )
        except RuntimeError as e:
            # Same answer as every other producer: the file panel keys its error
            # card off the 503's detail, so a fourth variant here would render a
            # different card for the same condition.
            logger.warning(
                f"Sandbox unreachable syncing {workspace_id}: {single_line(str(e))}"
            )
            raise sandbox_failure_http(e) from None
    if result.held:
        # The pass found the folder gone and offered its restore instead.
        raise HTTPException(status_code=409, detail=_RESTORE_OFFER_STANDS)
    return {
        "workspace_id": workspace_id,
        "synced": result.synced,
        "skipped": result.skipped,
        "deleted": result.deleted,
        "errors": result.errors,
        "oversized": result.oversized,
        "total_size": result.total_size,
        "max_file_bytes": result.max_file_bytes,
        "unsaved": [asdict(f) for f in result.unsaved[:_UNSAVED_LISTED]],
        "unsaved_count": len(result.unsaved),
    }


@router.get("/{workspace_id}/files/backup-status")
async def get_backup_status(
    workspace_id: str,
    x_user_id: CurrentUserId,
) -> dict[str, Any]:
    """Get backup status: compare sandbox files against DB to show what's
    backed up, modified, or untracked.

    ``files_restore_incomplete`` rides along on every branch: a file this
    status reports as backed up can still be absent from the folder after a
    restore that could not recover it, and without the flag that reads as an
    ordinary missing file.
    """

    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=x_user_id)
    restore_incomplete = bool((workspace or {}).get("files_restore_incomplete"))
    # Ride along like the flag: off, nothing reported unsaved will be saved,
    # and with an offer standing nothing is saved until the owner chooses.
    choice = {
        "files_restore_incomplete": restore_incomplete,
        "files_backup": await _computer_backs_up_files(workspace),
        "files_restore_offered_at": (workspace or {}).get("files_restore_offered_at"),
    }

    empty = {
        "workspace_id": workspace_id,
        "backed_up": [],
        "modified": [],
        "untracked": [],
        "total_backed_up_size": 0,
        **choice,
    }

    if is_unbound(workspace):
        return empty

    from src.server.database.workspace_file import (
        get_file_metadata_for_sync,
        get_workspace_total_size,
    )

    # The sync metadata carries directory and symlink rows too; this status
    # is about files, and the running branch below only ever sees files.
    db_meta = {
        path: meta
        for path, meta in (await get_file_metadata_for_sync(workspace_id)).items()
        if meta.get("kind", "file") == "file"
    }

    # If sandbox is stopped, everything in DB is "backed_up", nothing else
    if served_from_mirror(workspace.get("status")):
        total_size = await get_workspace_total_size(workspace_id)
        return {
            "workspace_id": workspace_id,
            "backed_up": list(db_meta.keys()),
            "modified": [],
            "untracked": [],
            "total_backed_up_size": total_size,
            **choice,
        }

    # Sandbox is running: compare sandbox files against DB. Not ready, folder
    # moving or a failed scan all return DB-only info.
    try:
        async with acquire_sandbox_to_change(workspace_id, x_user_id) as (sandbox, workspace):
            sandbox_meta = await FilePersistenceService.list_sandbox_files(
                sandbox, layout=owner_layout(workspace)
            )
    except Exception:
        total_size = await get_workspace_total_size(workspace_id)
        return {
            "workspace_id": workspace_id,
            "backed_up": list(db_meta.keys()),
            "modified": [],
            "untracked": [],
            "total_backed_up_size": total_size,
            **choice,
        }

    backed_up: list[str] = []
    modified: list[str] = []
    untracked: list[str] = []

    for virtual_path, info in sandbox_meta.items():
        db_entry = db_meta.get(virtual_path)
        if db_entry is None:
            untracked.append(virtual_path)
        else:
            size_match = db_entry["file_size"] == info["file_size"]
            mtime_match = (
                db_entry["mtime_epoch"] is not None
                and info["mtime"] > 0
                and abs(db_entry["mtime_epoch"] - info["mtime"]) < 1.0
            )
            if size_match and mtime_match:
                backed_up.append(virtual_path)
            else:
                modified.append(virtual_path)

    total_size = await get_workspace_total_size(workspace_id)

    return {
        "workspace_id": workspace_id,
        "backed_up": backed_up,
        "modified": modified,
        "untracked": untracked,
        "total_backed_up_size": total_size,
        **choice,
    }


class DeleteFilesRequest(BaseModel):
    paths: list[str] = Field(..., min_length=1, max_length=100)


@router.delete("/{workspace_id}/files")
async def delete_workspace_files(
    workspace_id: str,
    x_user_id: CurrentUserId,
    body: DeleteFilesRequest = Body(...),
) -> dict[str, Any]:
    """Delete one or more files from the workspace's live sandbox."""

    workspace = await owned_workspace_on_computer(workspace_id, x_user_id)

    if served_from_mirror(workspace.get("status")):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete files — workspace is {workspace.get('status')}. Wait for it to be running.",
        )

    async with acquire_sandbox_to_change(workspace_id, x_user_id) as (sandbox, workspace):
        work_dir = owner_work_dir(workspace)
        previous_dirs = previous_dir_names_of(workspace)

        errors: list[dict[str, str]] = []
        valid_paths: list[tuple[str, str]] = []  # (normalized, client_path)

        # One probe for the whole batch. This route takes up to a hundred paths,
        # and a probe apiece is a hundred sandbox round trips for one click.
        probe_slot: dict[int, int] = {}
        candidates: list[str] = []
        undeletable: dict[int, str] = {}
        for index, path in enumerate(body.paths):
            candidate = contained_absolute_path(path, work_dir, previous_dirs)
            if candidate is None or not sandbox.validate_path(candidate):
                continue
            # A server-held file is refused for what it is, before the probe:
            # with the file mount down its link resolves to nothing, and the
            # file the panel lists would read as not found.
            virtual = _virtual_file(_to_client_path(sandbox, candidate, work_dir))
            if virtual:
                undeletable[index] = virtual.undeletable
                continue
            probe_slot[index] = len(candidates)
            candidates.append(candidate)
        canonicals = await contained_sandbox_paths(sandbox, candidates, work_dir=work_dir)

        for index, path in enumerate(body.paths):
            if index in undeletable:
                errors.append({"path": path, "detail": undeletable[index]})
                continue
            slot = probe_slot.get(index)
            normalized = canonicals[slot] if slot is not None else None
            if normalized is None:
                errors.append({"path": path, "detail": "File not found"})
                continue

            addressed = candidates[slot]
            client_path = _to_client_path(sandbox, addressed, work_dir)
            if _is_system_path(client_path):
                errors.append({"path": path, "detail": "Cannot delete system files"})
                continue

            # The canonical path proves the request stays inside this project. The
            # object to unlink is still the addressed path: using the canonical
            # target here would follow a symlink and delete the file behind it.
            valid_paths.append((addressed, client_path))

        deleted: list[str] = []
        if valid_paths:
            # ``rm -f`` through the shell bypasses the write path, so it has to take
            # the same per-path locks an upload takes or it can land between another
            # writer's read and write. Acquired in sorted order, and only ever in
            # that order, so two batches that overlap cannot deadlock on each other.
            async with AsyncExitStack() as locks:
                for normalized, _ in sorted(valid_paths):
                    await locks.enter_async_context(sandbox.path_write_lock(normalized))

                rm_args = " ".join(shlex.quote(p) for p, _ in valid_paths)
                result = await sandbox.execute_bash_command(f"rm -f {rm_args}")
                if result.get("success"):
                    deleted = [cp for _, cp in valid_paths]
                else:
                    # Batch failed: fall back to per-file delete
                    for normalized, client_path in valid_paths:
                        r = await sandbox.execute_bash_command(
                            f"rm -f {shlex.quote(normalized)}"
                        )
                        if r.get("success"):
                            deleted.append(client_path)
                        else:
                            errors.append(
                                {
                                    "path": client_path,
                                    "detail": r.get("stderr", "Delete failed"),
                                }
                            )

    return {"deleted": deleted, "errors": errors}
