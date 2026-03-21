"""Workspace Files API Router.

Provides file operations against a workspace's Daytona sandbox, with DB
fallback for stopped workspaces (offline file access).

Design goals:
- Proxy all file access through the backend (UI clients never talk to Daytona directly).
- Auto-start stopped workspaces for write operations.
- Serve files from PostgreSQL when sandbox is stopped (read-only).
- Support both virtual paths ("results/foo.txt") and absolute sandbox paths
  ("/home/workspace/results/foo.txt").
- Return virtual paths to clients for a consistent UX.

Endpoints:
- GET    /api/v1/workspaces/{workspace_id}/files
- GET    /api/v1/workspaces/{workspace_id}/files/read
- PUT    /api/v1/workspaces/{workspace_id}/files/write
- GET    /api/v1/workspaces/{workspace_id}/files/download
- POST   /api/v1/workspaces/{workspace_id}/files/upload
- DELETE /api/v1/workspaces/{workspace_id}/files
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
import shlex
from typing import Any

from fastapi import APIRouter, Body, File, HTTPException, Query, Request, UploadFile
from pydantic import BaseModel, Field

from src.server.utils.api import CurrentUserId, require_workspace_owner
from fastapi.responses import Response

from ptc_agent.core.paths import (
    AGENT_SYSTEM_DIRS,
    ALWAYS_HIDDEN_BASENAMES as _SHARED_BASENAMES,
    ALWAYS_HIDDEN_PATH_SEGMENTS,
    ALWAYS_HIDDEN_SUFFIXES,
    HIDDEN_DIR_NAMES,
)
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.services.workspace_manager import WorkspaceManager
from src.server.services.persistence.file import FilePersistenceService
from src.server.utils.secret_redactor import get_redactor, get_vault_secrets_for_redaction

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["Workspace Files"])

# Image MIME types that benefit from HTTP caching
_CACHEABLE_IMAGE_TYPES = frozenset(
    {
        "image/png",
        "image/jpeg",
        "image/gif",
        "image/svg+xml",
        "image/webp",
    }
)

# Derived from shared constants (source of truth: ptc_agent.core.paths)
_SYSTEM_DIR_PREFIXES = tuple(f"{d}/" for d in sorted(AGENT_SYSTEM_DIRS))
_HIDDEN_DIR_PREFIXES = tuple(f"{d}/" for d in sorted(HIDDEN_DIR_NAMES))
_ALWAYS_HIDDEN_SEGMENTS = ALWAYS_HIDDEN_PATH_SEGMENTS
_ALWAYS_HIDDEN_BASENAMES = _SHARED_BASENAMES + (".file_sync_marker",)
_ALWAYS_HIDDEN_SUFFIXES = ALWAYS_HIDDEN_SUFFIXES

_ALWAYS_HIDDEN_DIR_PREFIXES = (
    # Package managers / dependencies
    "node_modules/",
    ".venv/",
    "venv/",
    "vendor/",
    # Build artifacts
    ".next/",
    ".nuxt/",
    # Caches
    ".cache/",
    ".pytest_cache/",
    ".mypy_cache/",
    ".ruff_cache/",
    # VCS
    ".git/",
)

# Generous but bounded defaults.
DEFAULT_READ_LIMIT_LINES = 20_000
MAX_UPLOAD_BYTES = 250 * 1024 * 1024  # 250MB

# Known binary file extensions that cannot be read as text
_BINARY_EXTENSIONS = frozenset(
    {
        ".pdf",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".bmp",
        ".ico",
        ".tiff",
        ".zip",
        ".tar",
        ".gz",
        ".bz2",
        ".7z",
        ".rar",
        ".exe",
        ".dll",
        ".so",
        ".dylib",
        ".mp3",
        ".mp4",
        ".wav",
        ".avi",
        ".mov",
        ".mkv",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".ppt",
        ".pptx",
        ".sqlite",
        ".db",
        ".pickle",
        ".pkl",
    }
)


def _is_binary(path: str) -> bool:
    """Check if file extension suggests binary content."""
    suffix = path.rsplit(".", 1)[-1].lower() if "." in path else ""
    return f".{suffix}" in _BINARY_EXTENSIONS


def _is_flash_workspace(workspace: dict[str, Any]) -> bool:
    return workspace.get("status") == "flash"


async def _acquire_sandbox(workspace_id: str, user_id: str) -> Any:
    """Get a ready sandbox for the workspace, or raise 503."""
    manager = WorkspaceManager.get_instance()
    try:
        session = await manager.get_session_for_workspace(workspace_id, user_id=user_id)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Sandbox not ready: {e}") from None

    sandbox = getattr(session, "sandbox", None)
    if sandbox is None:
        raise HTTPException(status_code=503, detail="Sandbox not available")
    return sandbox


def _to_client_path(sandbox: Any, absolute_path: str) -> str:
    """Convert an absolute sandbox path into a virtual client path.

    The CLI and web UX prefer paths like "results/foo.txt" (no leading slash),
    while still preserving true absolute /tmp paths.
    """

    virtual_path = sandbox.virtualize_path(absolute_path)

    # Keep /tmp paths absolute.
    if virtual_path.startswith("/tmp/"):
        return virtual_path

    # Strip the leading slash for working-directory paths.
    if virtual_path.startswith("/"):
        return virtual_path[1:]

    return virtual_path


def _is_system_path(client_path: str) -> bool:
    return any(client_path.startswith(prefix) for prefix in _SYSTEM_DIR_PREFIXES)


def _is_hidden_path(client_path: str) -> bool:
    if client_path == "_internal":
        return True
    return any(client_path.startswith(prefix) for prefix in _HIDDEN_DIR_PREFIXES)


def _is_always_hidden_path(client_path: str) -> bool:
    normalized = f"/{client_path.lstrip('/')}"

    if normalized.endswith(_ALWAYS_HIDDEN_BASENAMES):
        return True

    if normalized.endswith(_ALWAYS_HIDDEN_SUFFIXES):
        return True

    if any(seg in normalized for seg in _ALWAYS_HIDDEN_SEGMENTS):
        return True

    if any(client_path.startswith(prefix) for prefix in _ALWAYS_HIDDEN_DIR_PREFIXES):
        return True

    return False


def _get_work_dir() -> str:
    """Return the configured working directory from WorkspaceManager config."""
    manager = WorkspaceManager.get_instance()
    return manager.config.to_core_config().filesystem.working_directory


def _normalize_requested_path(path: str, work_dir: str) -> str:
    """Normalize a requested path for comparison."""
    raw = (path or "").strip()
    if raw in {"", ".", "./"}:
        return ""

    normalized = raw
    work_dir_prefix = work_dir.rstrip("/") + "/"
    if normalized.startswith(work_dir_prefix):
        normalized = normalized[len(work_dir_prefix):]
    if normalized.startswith("/"):
        normalized = normalized[1:]
    if normalized.startswith("./"):
        normalized = normalized[2:]

    return normalized


def _requested_hidden_ok(path: str, work_dir: str) -> bool:
    """Return True if caller explicitly requested a hidden directory."""
    normalized = _normalize_requested_path(path, work_dir)
    if not normalized:
        return False
    return normalized == "_internal" or normalized.startswith("_internal/")


def _requested_system_ok(path: str, work_dir: str) -> bool:
    """Return True if caller explicitly requested a system directory."""
    normalized = _normalize_requested_path(path, work_dir)
    if not normalized:
        return False
    return any(
        normalized == prefix.rstrip("/") or normalized.startswith(prefix)
        for prefix in _SYSTEM_DIR_PREFIXES
    )


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

    if _is_flash_workspace(workspace):
        return {"files": [], "sandbox_ready": False, "flash_workspace": True}

    work_dir = _get_work_dir()

    # DB fallback for stopped workspaces (unless auto_start requested)
    if not auto_start and workspace.get("status") in ("stopped", "stopping"):
        file_tree = await FilePersistenceService.get_file_tree(workspace_id)
        # Filter by path prefix if specified
        normalized_path = _normalize_requested_path(path, work_dir)
        if normalized_path:
            file_tree = [
                f
                for f in file_tree
                if f["path"].startswith(normalized_path + "/")
                or f["path"] == normalized_path
            ]
        allow_hidden = _requested_hidden_ok(path, work_dir)
        files = [
            f["path"]
            for f in file_tree
            if not _is_always_hidden_path(f["path"])
            and (include_system or not _is_system_path(f["path"]))
            and (allow_hidden or not _is_hidden_path(f["path"]))
        ]
        return {
            "workspace_id": workspace_id,
            "path": path,
            "files": files,
            "sandbox_ready": False,
            "source": "database",
        }

    sandbox = await _acquire_sandbox(workspace_id, x_user_id)

    # Fast path: return empty list if sandbox is still initializing and wait_for_sandbox=False
    # This allows CLI autocomplete to populate later without blocking startup
    if not wait_for_sandbox and not sandbox.is_ready():
        return {"files": [], "sandbox_ready": False}

    # Pre-check sandbox health before file listing.
    # aglob_files swallows all exceptions and returns [], which turns a broken
    # sandbox into "200 with no files". This check surfaces the real error.
    try:
        await sandbox.ensure_sandbox_ready()
    except Exception as e:
        logger.warning(f"Sandbox health check failed for workspace {workspace_id}: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Sandbox is not reachable: {e}",
        )

    # aglob_files returns absolute sandbox paths.
    # Allow explicit listing of hidden internal paths (e.g. /view _internal/...).
    allow_denied = _requested_hidden_ok(path, work_dir)
    try:
        absolute_paths: list[str] = await sandbox.aglob_files(
            pattern, path=path, allow_denied=allow_denied
        )
    except RuntimeError:
        raise HTTPException(status_code=503, detail="Sandbox is still starting")

    allow_hidden = _requested_hidden_ok(path, work_dir)

    files: list[str] = []
    for absolute_path in absolute_paths:
        client_path = _to_client_path(sandbox, absolute_path)

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
            and not _requested_system_ok(path, work_dir)
        ):
            continue

        files.append(client_path)

    return {
        "workspace_id": workspace_id,
        "path": path,
        "files": files,
        "sandbox_ready": True,
    }


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

    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=x_user_id)

    if _is_flash_workspace(workspace):
        raise HTTPException(
            status_code=400, detail="Flash workspaces do not have a sandbox"
        )

    # DB fallback for stopped workspaces
    if workspace.get("status") in ("stopped", "stopping"):
        work_dir = _get_work_dir()
        normalized_path = _normalize_requested_path(path, work_dir)
        if not normalized_path:
            raise HTTPException(status_code=400, detail="File path is required")

        # Parallel: fetch vault secrets + file content in one round-trip window
        vault_secrets, file_record = await asyncio.gather(
            get_vault_secrets_for_redaction(workspace_id),
            FilePersistenceService.get_file_content(workspace_id, normalized_path),
        )
        if not file_record:
            raise HTTPException(status_code=404, detail="File not found")

        if file_record.get("is_binary"):
            raise HTTPException(
                status_code=415,
                detail="Cannot read binary file as text. Use GET /files/download instead.",
            )

        text_content = file_record.get("content_text", "")
        text_content = get_redactor().redact(text_content, vault_secrets=vault_secrets)
        if unlimited:
            content = text_content
        else:
            lines = text_content.splitlines()
            content = "\n".join(lines[offset : offset + limit])
        mime = file_record.get("mime_type") or "text/plain"

        return {
            "workspace_id": workspace_id,
            "path": normalized_path,
            "offset": offset,
            "limit": limit,
            "content": content,
            "mime": mime,
            "truncated": False,
            "source": "database",
        }

    sandbox = await _acquire_sandbox(workspace_id, x_user_id)

    normalized, error = sandbox.validate_and_normalize_path(path)
    if error:
        raise HTTPException(status_code=403, detail=error)

    # Download raw bytes first to distinguish "not found" from "binary file"
    try:
        raw_bytes = await sandbox.adownload_file_bytes(normalized)
    except RuntimeError:
        raise HTTPException(status_code=503, detail="Sandbox is still starting")
    if raw_bytes is None:
        raise HTTPException(status_code=404, detail="File not found")

    # Check for known binary extensions
    if _is_binary(normalized):
        raise HTTPException(
            status_code=415,
            detail="Cannot read binary file as text. Use GET /files/download instead.",
        )

    # Try to decode as UTF-8
    try:
        text_content = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(
            status_code=415,
            detail="File appears to be binary and cannot be read as text. Use GET /files/download instead.",
        )

    vault_secrets = await get_vault_secrets_for_redaction(workspace_id)
    text_content = get_redactor().redact(text_content, vault_secrets=vault_secrets)

    # Apply line range (skip when unlimited=True for edit mode)
    if unlimited:
        content = text_content
    else:
        lines = text_content.splitlines()
        content = "\n".join(lines[offset : offset + limit])

    client_path = _to_client_path(sandbox, normalized)
    if _is_always_hidden_path(client_path):
        raise HTTPException(status_code=404, detail="File not found")

    mime, _enc = mimetypes.guess_type(client_path)

    return {
        "workspace_id": workspace_id,
        "path": client_path,
        "offset": offset,
        "limit": limit,
        "content": content,
        "mime": mime or "text/plain",
        "truncated": False,  # limit is enforced; UI can request more with offset.
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

    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=x_user_id)

    if _is_flash_workspace(workspace):
        raise HTTPException(
            status_code=400, detail="Flash workspaces do not have a sandbox"
        )

    if workspace.get("status") in ("stopped", "stopping"):
        raise HTTPException(
            status_code=409,
            detail="Cannot write files while workspace is stopped. Start the workspace first.",
        )

    content_bytes = body.content.encode("utf-8")
    if len(content_bytes) > MAX_WRITE_BYTES:
        raise HTTPException(status_code=413, detail="File content too large (max 10MB)")

    sandbox = await _acquire_sandbox(workspace_id, x_user_id)

    normalized, error = sandbox.validate_and_normalize_path(path)
    if error:
        raise HTTPException(status_code=403, detail=error)

    try:
        ok = await sandbox.awrite_file_text(normalized, body.content)
    except RuntimeError:
        raise HTTPException(status_code=503, detail="Sandbox is still starting")
    if not ok:
        raise HTTPException(status_code=500, detail="Write failed")

    # Invalidate agent.md cache when user edits agent.md via UI
    client_path = _to_client_path(sandbox, normalized)
    if client_path == "agent.md":
        try:
            manager = WorkspaceManager.get_instance()
            session = manager._sessions.get(workspace_id)
            if session:
                session.invalidate_agent_md()
        except Exception:
            pass

    return {
        "workspace_id": workspace_id,
        "path": client_path,
        "size": len(content_bytes),
    }


def _build_download_response(
    content: bytes, filename: str, mime: str, request: Request
) -> Response:
    """Build a download response with caching headers for image types."""
    etag = hashlib.md5(content).hexdigest()
    safe_filename = filename.replace('"', '\\"')
    headers: dict[str, str] = {
        "Content-Disposition": f'inline; filename="{safe_filename}"',
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

    return Response(
        content=content,
        media_type=mime,
        headers=headers,
    )


@router.get("/{workspace_id}/files/download")
async def download_workspace_file(
    workspace_id: str,
    x_user_id: CurrentUserId,
    request: Request,
    path: str = Query(..., description="File path (virtual or absolute)."),
) -> Response:
    """Download raw bytes from the workspace's sandbox, or from DB if stopped."""

    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=x_user_id)

    if _is_flash_workspace(workspace):
        raise HTTPException(
            status_code=400, detail="Flash workspaces do not have a sandbox"
        )

    # DB fallback for stopped workspaces
    if workspace.get("status") in ("stopped", "stopping"):
        work_dir = _get_work_dir()
        normalized_path = _normalize_requested_path(path, work_dir)
        if not normalized_path:
            raise HTTPException(status_code=400, detail="File path is required")

        # Parallel: fetch vault secrets + file content in one round-trip window
        vault_secrets, file_record = await asyncio.gather(
            get_vault_secrets_for_redaction(workspace_id),
            FilePersistenceService.get_file_content(workspace_id, normalized_path),
        )
        if not file_record:
            raise HTTPException(status_code=404, detail="File not found")

        if file_record.get("is_binary") and file_record.get("content_binary"):
            content = file_record["content_binary"]
            if isinstance(content, memoryview):
                content = bytes(content)
        elif file_record.get("content_text") is not None:
            content = file_record["content_text"].encode("utf-8")
        else:
            raise HTTPException(status_code=404, detail="File content not available")

        filename = file_record.get("file_name", "download")
        mime = file_record.get("mime_type") or "application/octet-stream"

        if mime and mime.startswith("text/"):
            content = get_redactor().redact_bytes(content, vault_secrets=vault_secrets)

        return _build_download_response(content, filename, mime, request)

    sandbox = await _acquire_sandbox(workspace_id, x_user_id)

    normalized, error = sandbox.validate_and_normalize_path(path)
    if error:
        raise HTTPException(status_code=403, detail=error)

    try:
        content = await sandbox.adownload_file_bytes(normalized)
    except RuntimeError:
        raise HTTPException(status_code=503, detail="Sandbox is still starting")
    if content is None:
        raise HTTPException(status_code=404, detail="File not found")

    client_path = _to_client_path(sandbox, normalized)
    if _is_always_hidden_path(client_path):
        raise HTTPException(status_code=404, detail="File not found")

    filename = client_path.split("/")[-1] if client_path else "download"
    mime, _enc = mimetypes.guess_type(filename)

    if mime and mime.startswith("text/"):
        vault_secrets = await get_vault_secrets_for_redaction(workspace_id)
        content = get_redactor().redact_bytes(content, vault_secrets=vault_secrets)

    return _build_download_response(
        content, filename, mime or "application/octet-stream", request
    )


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

    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=x_user_id)

    if _is_flash_workspace(workspace):
        raise HTTPException(
            status_code=400, detail="Flash workspaces do not have a sandbox"
        )

    if workspace.get("status") in ("stopped", "stopping"):
        raise HTTPException(
            status_code=409,
            detail="Cannot upload files while workspace is stopped. Start the workspace first.",
        )

    sandbox = await _acquire_sandbox(workspace_id, x_user_id)

    dest = path or file.filename
    if not dest:
        raise HTTPException(status_code=400, detail="Destination path is required")

    normalized, error = sandbox.validate_and_normalize_path(dest)
    if error:
        raise HTTPException(status_code=403, detail=error)

    content = await file.read()
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        limit_mb = MAX_UPLOAD_BYTES // (1024 * 1024)
        raise HTTPException(
            status_code=413,
            detail=f"File is too large ({size_mb:.1f} MB). Maximum upload size is {limit_mb} MB.",
        )

    try:
        ok = await sandbox.aupload_file_bytes(normalized, content)
    except RuntimeError:
        raise HTTPException(status_code=503, detail="Sandbox is still starting")
    if not ok:
        raise HTTPException(status_code=500, detail="Upload failed")

    client_path = _to_client_path(sandbox, normalized)
    return {
        "workspace_id": workspace_id,
        "path": client_path,
        "size": len(content),
        "filename": file.filename,
    }


@router.post("/{workspace_id}/files/backup")
async def backup_workspace_files(
    workspace_id: str,
    x_user_id: CurrentUserId,
) -> dict[str, Any]:
    """Backup workspace files from sandbox to DB for offline access."""

    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=x_user_id)

    if _is_flash_workspace(workspace):
        raise HTTPException(
            status_code=400, detail="Flash workspaces do not have a sandbox"
        )

    if workspace.get("status") in ("stopped", "stopping"):
        raise HTTPException(
            status_code=409,
            detail="Cannot backup files while workspace is stopped.",
        )

    sandbox = await _acquire_sandbox(workspace_id, x_user_id)

    try:
        result = await FilePersistenceService.sync_to_db(workspace_id, sandbox)
    except RuntimeError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Sandbox not ready: {e}",
        )
    return {
        "workspace_id": workspace_id,
        "synced": result["synced"],
        "skipped": result["skipped"],
        "deleted": result["deleted"],
        "errors": result["errors"],
        "total_size": result["total_size"],
    }


@router.get("/{workspace_id}/files/backup-status")
async def get_backup_status(
    workspace_id: str,
    x_user_id: CurrentUserId,
) -> dict[str, Any]:
    """Get backup status: compare sandbox files against DB to show what's
    backed up, modified, or untracked."""

    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=x_user_id)

    empty = {
        "workspace_id": workspace_id,
        "backed_up": [],
        "modified": [],
        "untracked": [],
        "total_backed_up_size": 0,
    }

    if _is_flash_workspace(workspace):
        return empty

    from src.server.database.workspace_file import (
        get_file_metadata_for_sync,
        get_workspace_total_size,
    )

    db_meta = await get_file_metadata_for_sync(workspace_id)

    # If sandbox is stopped, everything in DB is "backed_up", nothing else
    if workspace.get("status") in ("stopped", "stopping"):
        total_size = await get_workspace_total_size(workspace_id)
        return {
            "workspace_id": workspace_id,
            "backed_up": list(db_meta.keys()),
            "modified": [],
            "untracked": [],
            "total_backed_up_size": total_size,
        }

    # Sandbox is running — compare sandbox files against DB
    try:
        sandbox = await _acquire_sandbox(workspace_id, x_user_id)
    except HTTPException:
        # Sandbox not ready — return DB-only info
        total_size = await get_workspace_total_size(workspace_id)
        return {
            "workspace_id": workspace_id,
            "backed_up": list(db_meta.keys()),
            "modified": [],
            "untracked": [],
            "total_backed_up_size": total_size,
        }

    # Run find to get current sandbox file metadata
    try:
        sandbox_meta = await FilePersistenceService.list_sandbox_files(sandbox)
    except Exception:
        total_size = await get_workspace_total_size(workspace_id)
        return {
            "workspace_id": workspace_id,
            "backed_up": list(db_meta.keys()),
            "modified": [],
            "untracked": [],
            "total_backed_up_size": total_size,
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

    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=x_user_id)

    if _is_flash_workspace(workspace):
        raise HTTPException(
            status_code=400, detail="Flash workspaces do not have a sandbox"
        )

    if workspace.get("status") in ("stopped", "stopping"):
        raise HTTPException(
            status_code=409,
            detail="Cannot delete files while workspace is stopped. Start the workspace first.",
        )

    sandbox = await _acquire_sandbox(workspace_id, x_user_id)

    errors: list[dict[str, str]] = []
    valid_paths: list[tuple[str, str]] = []  # (normalized, client_path)

    for path in body.paths:
        normalized, error = sandbox.validate_and_normalize_path(path)
        if error:
            errors.append({"path": path, "detail": error})
            continue

        client_path = _to_client_path(sandbox, normalized)
        if _is_system_path(client_path):
            errors.append({"path": path, "detail": "Cannot delete system files"})
            continue

        valid_paths.append((normalized, client_path))

    deleted: list[str] = []
    if valid_paths:
        rm_args = " ".join(shlex.quote(p) for p, _ in valid_paths)
        try:
            result = await sandbox.execute_bash_command(f"rm -f {rm_args}")
        except RuntimeError:
            raise HTTPException(status_code=503, detail="Sandbox is still starting")
        if result.get("success"):
            deleted = [cp for _, cp in valid_paths]
        else:
            # Batch failed — fall back to per-file delete
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
