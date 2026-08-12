"""Authenticated workspace file CRUD routes (`/api/v1/workspaces/{id}/files*`)."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import shlex
from typing import Any

from fastapi import APIRouter, Body, File, HTTPException, Query, Request, UploadFile
from pydantic import BaseModel, Field

from src.server.utils.api import CurrentUserId, require_workspace_owner
from src.server.utils.http_headers import content_disposition
from fastapi.responses import Response

from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.services.workspace_manager import WorkspaceManager
from src.server.services.persistence.file import FilePersistenceService
from src.server.utils.secret_redactor import get_redactor, get_vault_secrets_for_redaction
from src.utils.mime import resolve_content_type

from ._shared import (
    DEFAULT_READ_LIMIT_LINES,
    _USER_PROFILE_FILES,
    _is_text_content_type,
    _is_utf8,
    MAX_UPLOAD_BYTES,
    _CACHEABLE_IMAGE_TYPES,
    _USER_PROFILE_PREFIX,
    _acquire_sandbox,
    _decode_file_text,
    _get_work_dir,
    _is_always_hidden_path,
    _is_binary,
    _is_flash_workspace,
    _is_hidden_path,
    _is_system_path,
    _is_user_profile_file,
    _normalize_requested_path,
    _record_fs_bytes,
    _requested_hidden_ok,
    _requested_system_ok,
    _serialize_user_profile_file,
    _to_client_path,
)

logger = logging.getLogger(__name__)



router = APIRouter(prefix="/api/v1/workspaces", tags=["Workspace Files"])


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
    if not auto_start and workspace.get("status") in ("stopped", "stopping", "starting"):
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

    # Allow explicit listing of hidden internal paths (e.g. _internal/...).
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

    # Splice in the three virtual user-profile files when the request scope
    # includes .agents/user/profile/. They don't exist on the sandbox FS,
    # so aglob_files never returns them.
    requested_norm = _normalize_requested_path(path, work_dir)
    if (
        requested_norm == ""
        or _USER_PROFILE_PREFIX.startswith(f"{requested_norm}/")
        or _USER_PROFILE_PREFIX.rstrip("/") == requested_norm
        or requested_norm.startswith(_USER_PROFILE_PREFIX)
    ):
        for virtual_path in _USER_PROFILE_FILES:
            if virtual_path not in files:
                files.append(virtual_path)

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

    # Virtual user-profile JSON files — served from DB, independent of sandbox state.
    # Works whether the workspace is running, stopped, or never started.
    work_dir = _get_work_dir()
    normalized_for_profile = _normalize_requested_path(path, work_dir)
    if _is_user_profile_file(normalized_for_profile):
        try:
            text_content = await _serialize_user_profile_file(normalized_for_profile, x_user_id)
        except Exception:
            logger.exception(
                "user-profile virtual read failed", extra={"path": normalized_for_profile}
            )
            raise HTTPException(status_code=500, detail="Failed to read user profile data")
        if unlimited:
            content = text_content
            truncated = False
        else:
            lines = text_content.splitlines()
            content = "\n".join(lines[offset : offset + limit])
            truncated = len(lines) > offset + limit
        return {
            "workspace_id": workspace_id,
            "path": normalized_for_profile,
            "offset": offset,
            "limit": limit,
            "content": content,
            "mime": "application/json",
            "truncated": truncated,
            "source": "user_data_backend",
        }

    # DB fallback for stopped workspaces
    if workspace.get("status") in ("stopped", "stopping", "starting"):
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

    decoded = _decode_file_text(raw_bytes)
    if decoded is None:
        raise HTTPException(
            status_code=415,
            detail="File appears to be binary and cannot be read as text. Use GET /files/download instead.",
        )
    text_content = decoded

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

    mime = resolve_content_type(client_path, default="text/plain")

    _record_fs_bytes("read", len(raw_bytes))

    return {
        "workspace_id": workspace_id,
        "path": client_path,
        "offset": offset,
        "limit": limit,
        "content": content,
        "mime": mime,
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

    if workspace.get("status") in ("stopped", "stopping", "starting"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot write files — workspace is {workspace.get('status')}. Wait for it to be running.",
        )

    # User-profile virtual files are read-only through this API. Writes happen
    # via the dashboard widgets (portfolio/watchlist CRUD) or the agent's
    # CompositeFilesystemBackend → UserDataBackend route, both of which apply
    # schema validation and version checks that this generic write endpoint
    # cannot enforce safely.
    work_dir = _get_work_dir()
    normalized_for_profile = _normalize_requested_path(path, work_dir)
    if _is_user_profile_file(normalized_for_profile):
        raise HTTPException(
            status_code=400,
            detail=(
                "User-profile JSON files are read-only through the file panel. "
                "Edit via the dashboard widget or ask the agent to update it."
            ),
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

    _record_fs_bytes("write", len(content_bytes))

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
    headers: dict[str, str] = {
        "Content-Disposition": content_disposition(filename, disposition="inline"),
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
    if workspace.get("status") in ("stopped", "stopping", "starting"):
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

        if _is_text_content_type(mime) or _is_utf8(content):
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
    mime = resolve_content_type(filename)

    if _is_text_content_type(mime) or _is_utf8(content):
        vault_secrets = await get_vault_secrets_for_redaction(workspace_id)
        content = get_redactor().redact_bytes(content, vault_secrets=vault_secrets)

    _record_fs_bytes("download", len(content))

    return _build_download_response(content, filename, mime, request)


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

    if workspace.get("status") in ("stopped", "stopping", "starting"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot upload files — workspace is {workspace.get('status')}. Wait for it to be running.",
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
    _record_fs_bytes("upload", len(content))
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

    if workspace.get("status") in ("stopped", "stopping", "starting"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot backup files — workspace is {workspace.get('status')}.",
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
    if workspace.get("status") in ("stopped", "stopping", "starting"):
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

    if workspace.get("status") in ("stopped", "stopping", "starting"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete files — workspace is {workspace.get('status')}. Wait for it to be running.",
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
        if _is_user_profile_file(client_path):
            errors.append({
                "path": path,
                "detail": (
                    "User-profile JSON files cannot be deleted through the file panel. "
                    "Manage entries via the dashboard widgets."
                ),
            })
            continue
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
