"""
Public Share Router — Unauthenticated endpoints for shared thread access.

All endpoints use an opaque share_token instead of thread/workspace IDs.
No auth required. workspace_id is resolved server-side and never exposed.

Endpoints:
- GET /api/v1/public/shared/{share_token}          — Thread metadata
- GET /api/v1/public/shared/{share_token}/replay    — SSE conversation replay
- GET /api/v1/public/shared/{share_token}/files     — File listing (requires allow_files)
- GET /api/v1/public/shared/{share_token}/files/read     — Read file content (requires allow_files)
- GET /api/v1/public/shared/{share_token}/files/download — Download raw file (requires allow_download)
"""

import asyncio
import json
import logging
import mimetypes
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from src.server.utils.secret_redactor import get_redactor, get_vault_secrets_for_redaction

from src.server.database.conversation import (
    get_thread_by_share_token,
    get_queries_for_thread,
    get_responses_for_thread,
)
from src.server.app.workspace_files import (
    _is_always_hidden_path,
    _is_hidden_path,
    _is_system_path,
    _normalize_requested_path,
    _is_binary,
    DEFAULT_READ_LIMIT_LINES,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/public", tags=["Public Sharing"])


async def _get_shared_thread(share_token: str) -> dict[str, Any]:
    """Fetch shared thread or raise 404."""
    thread = await get_thread_by_share_token(share_token)
    if not thread:
        raise HTTPException(status_code=404, detail="Shared thread not found")
    return thread


def _get_permissions(thread: dict[str, Any]) -> dict[str, Any]:
    """Extract permissions dict from thread record."""
    perms = thread.get("share_permissions") or {}
    if isinstance(perms, str):
        perms = json.loads(perms)
    return perms


def _require_permission(perms: dict[str, Any], key: str) -> None:
    """Raise 403 if a specific permission is not granted."""
    if not perms.get(key):
        raise HTTPException(status_code=403, detail=f"Permission '{key}' not granted for this shared thread")


# =============================================================================
# METADATA
# =============================================================================


@router.get("/shared/{share_token}")
async def get_shared_thread_metadata(share_token: str):
    """Get metadata for a shared thread. No auth required."""
    thread = await _get_shared_thread(share_token)
    perms = _get_permissions(thread)

    return {
        "thread_id": str(thread["conversation_thread_id"]),
        "title": thread.get("title"),
        "msg_type": thread.get("msg_type"),
        "created_at": thread.get("created_at"),
        "updated_at": thread.get("updated_at"),
        "workspace_name": thread.get("workspace_name"),
        "permissions": {
            "allow_files": perms.get("allow_files", False),
            "allow_download": perms.get("allow_download", False),
        },
    }


# =============================================================================
# REPLAY
# =============================================================================


@router.get("/shared/{share_token}/replay")
async def replay_shared_thread(share_token: str):
    """Replay a shared thread as SSE. No auth required.

    Same replay logic as the authenticated endpoint, but resolves
    thread via share_token and strips sensitive fields.
    """
    thread = await _get_shared_thread(share_token)
    thread_id = str(thread["conversation_thread_id"])

    queries, _ = await get_queries_for_thread(thread_id)
    responses, _ = await get_responses_for_thread(thread_id)
    responses_by_turn = {r.get("turn_index"): r for r in responses if isinstance(r, dict)}

    async def event_generator():
        seq = 0

        for q in queries:
            if not isinstance(q, dict):
                continue

            turn_index = q.get("turn_index")
            seq += 1

            # Build user_message payload, stripping workspace_id from metadata
            metadata = q.get("metadata") or {}
            if isinstance(metadata, dict):
                metadata = {k: v for k, v in metadata.items() if k != "workspace_id"}

            payload = {
                "thread_id": thread_id,
                "turn_index": turn_index,
                "content": q.get("content"),
                "timestamp": q.get("created_at"),
                "metadata": metadata,
            }
            # Tag system queries so the frontend can hide the user bubble
            query_type = q.get("type")
            if query_type == "system":
                payload["query_type"] = "system"

            yield (
                f"id: {seq}\n"
                f"event: user_message\n"
                f"data: {json.dumps(payload, ensure_ascii=False, default=str)}\n\n"
            )

            response = responses_by_turn.get(turn_index)
            if not response:
                continue

            sse_events = response.get("sse_events")
            if not (isinstance(sse_events, list) and sse_events):
                continue

            for item in sse_events:
                if not isinstance(item, dict):
                    continue
                event_type = item.get("event")
                data = item.get("data")
                if not event_type or not isinstance(data, dict):
                    continue

                seq += 1
                replay_data = dict(data)
                replay_data.setdefault("thread_id", thread_id)
                replay_data["turn_index"] = turn_index
                replay_data["response_id"] = str(response.get("conversation_response_id"))

                yield (
                    f"id: {seq}\n"
                    f"event: {event_type}\n"
                    f"data: {json.dumps(replay_data, ensure_ascii=False, default=str)}\n\n"
                )

        seq += 1
        yield f"id: {seq}\nevent: replay_done\ndata: {json.dumps({'thread_id': thread_id}, default=str)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"},
    )


# =============================================================================
# FILES (require permissions)
# =============================================================================

async def _get_shared_workspace_id(share_token: str, require_files: bool = False, require_download: bool = False) -> tuple[dict, str]:
    """Get thread + workspace_id for a shared file request, checking permissions."""
    thread = await _get_shared_thread(share_token)
    perms = _get_permissions(thread)

    if require_files:
        _require_permission(perms, "allow_files")
    if require_download:
        _require_permission(perms, "allow_download")

    return thread, str(thread["workspace_id"])


@router.get("/shared/{share_token}/files")
async def list_shared_files(
    share_token: str,
    path: str = Query(".", description="Directory to list."),
):
    """List files in a shared thread's workspace. Requires allow_files permission."""
    thread, workspace_id = await _get_shared_workspace_id(share_token, require_files=True)

    from src.server.database.workspace import get_workspace as db_get_workspace
    from src.server.services.persistence.file import FilePersistenceService
    from src.server.services.workspace_manager import WorkspaceManager

    workspace = await db_get_workspace(workspace_id)
    if not workspace:
        raise HTTPException(status_code=404, detail="Workspace not found")

    # Flash workspaces have no files
    if workspace.get("status") == "flash":
        return {"files": [], "source": "none"}

    # Try DB fallback first (works for stopped workspaces)
    # For public access, we prefer DB to avoid starting sandboxes
    file_tree = await FilePersistenceService.get_file_tree(workspace_id)

    normalized_path = _normalize_requested_path(path)
    if normalized_path:
        file_tree = [
            f for f in file_tree
            if f["path"].startswith(normalized_path + "/") or f["path"] == normalized_path
        ]

    files = []
    for f in file_tree:
        p = f["path"]
        if _is_always_hidden_path(p):
            continue
        if _is_hidden_path(p):
            continue
        if _is_system_path(p):
            continue
        files.append(p)

    if files:
        return {"path": path, "files": files, "source": "database"}

    # Try live sandbox if DB has no files
    if workspace.get("status") not in ("stopped", "stopping", "starting"):
        try:
            manager = WorkspaceManager.get_instance()
            session = await manager.get_session_for_workspace(workspace_id)
            sandbox = getattr(session, "sandbox", None)
            if sandbox:
                absolute_paths = await sandbox.aglob_files("**/*", path=path)
                from src.server.app.workspace_files import _to_client_path
                for ap in absolute_paths:
                    cp = _to_client_path(sandbox, ap)
                    if _is_always_hidden_path(cp) or _is_hidden_path(cp) or _is_system_path(cp):
                        continue
                    files.append(cp)
                return {"path": path, "files": files, "source": "sandbox"}
        except Exception:
            logger.debug(f"Sandbox not available for shared files in workspace {workspace_id}")

    return {"path": path, "files": files, "source": "database"}


@router.get("/shared/{share_token}/files/read")
async def read_shared_file(
    share_token: str,
    path: str = Query(..., description="File path to read."),
    offset: int = Query(0, ge=0, description="Line offset."),
    limit: int = Query(DEFAULT_READ_LIMIT_LINES, ge=1, le=DEFAULT_READ_LIMIT_LINES, description="Max lines."),
):
    """Read a text file from a shared thread's workspace. Requires allow_files permission."""
    thread, workspace_id = await _get_shared_workspace_id(share_token, require_files=True)

    from src.server.database.workspace import get_workspace as db_get_workspace
    from src.server.services.persistence.file import FilePersistenceService
    from src.server.services.workspace_manager import WorkspaceManager

    workspace = await db_get_workspace(workspace_id)
    if not workspace:
        raise HTTPException(status_code=404, detail="Workspace not found")

    normalized_path = _normalize_requested_path(path)
    if not normalized_path:
        raise HTTPException(status_code=400, detail="File path is required")

    if _is_always_hidden_path(normalized_path) or _is_hidden_path(normalized_path) or _is_system_path(normalized_path):
        raise HTTPException(status_code=404, detail="File not found")

    # Try DB first — parallel vault secrets + file content fetch
    vault_secrets, file_record = await asyncio.gather(
        get_vault_secrets_for_redaction(workspace_id),
        FilePersistenceService.get_file_content(workspace_id, normalized_path),
    )
    if file_record:
        if file_record.get("is_binary"):
            raise HTTPException(status_code=415, detail="Cannot read binary file as text.")

        text_content = file_record.get("content_text", "")
        text_content = get_redactor().redact(text_content, vault_secrets=vault_secrets)
        lines = text_content.splitlines()
        content = "\n".join(lines[offset:offset + limit])
        mime = file_record.get("mime_type") or "text/plain"

        return {
            "path": normalized_path,
            "offset": offset,
            "limit": limit,
            "content": content,
            "mime": mime,
            "truncated": False,
            "source": "database",
        }

    # Try live sandbox — vault secrets from session cache (instant)
    if workspace.get("status") not in ("stopped", "stopping", "starting"):
        try:
            manager = WorkspaceManager.get_instance()
            session = await manager.get_session_for_workspace(workspace_id)
            sandbox = getattr(session, "sandbox", None)
            if sandbox:
                norm, error = sandbox.validate_and_normalize_path(path)
                if error:
                    raise HTTPException(status_code=403, detail=error)

                raw_bytes = await sandbox.adownload_file_bytes(norm)
                if raw_bytes is None:
                    raise HTTPException(status_code=404, detail="File not found")

                if _is_binary(norm):
                    raise HTTPException(status_code=415, detail="Cannot read binary file as text.")

                try:
                    text_content = raw_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    raise HTTPException(status_code=415, detail="File appears to be binary.")

                text_content = get_redactor().redact(text_content, vault_secrets=vault_secrets)
                lines = text_content.splitlines()
                content = "\n".join(lines[offset:offset + limit])
                from src.server.app.workspace_files import _to_client_path
                client_path = _to_client_path(sandbox, norm)
                mime_type, _ = mimetypes.guess_type(client_path)

                return {
                    "path": client_path,
                    "offset": offset,
                    "limit": limit,
                    "content": content,
                    "mime": mime_type or "text/plain",
                    "truncated": False,
                    "source": "sandbox",
                }
        except HTTPException:
            raise
        except Exception:
            logger.debug(f"Sandbox not available for shared file read in workspace {workspace_id}")

    raise HTTPException(status_code=404, detail="File not found")


@router.get("/shared/{share_token}/files/download")
async def download_shared_file(
    share_token: str,
    path: str = Query(..., description="File path to download."),
):
    """Download a raw file from a shared thread's workspace. Requires allow_download permission."""
    thread, workspace_id = await _get_shared_workspace_id(share_token, require_download=True)

    from src.server.database.workspace import get_workspace as db_get_workspace
    from src.server.services.persistence.file import FilePersistenceService
    from src.server.services.workspace_manager import WorkspaceManager

    workspace = await db_get_workspace(workspace_id)
    if not workspace:
        raise HTTPException(status_code=404, detail="Workspace not found")

    normalized_path = _normalize_requested_path(path)
    if not normalized_path:
        raise HTTPException(status_code=400, detail="File path is required")

    if _is_always_hidden_path(normalized_path) or _is_hidden_path(normalized_path) or _is_system_path(normalized_path):
        raise HTTPException(status_code=404, detail="File not found")

    # Try DB first — parallel vault secrets + file content fetch
    vault_secrets, file_record = await asyncio.gather(
        get_vault_secrets_for_redaction(workspace_id),
        FilePersistenceService.get_file_content(workspace_id, normalized_path),
    )
    if file_record:
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

        return StreamingResponse(
            iter([content]),
            media_type=mime,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    # Try live sandbox — vault secrets from session cache (instant)
    if workspace.get("status") not in ("stopped", "stopping", "starting"):
        try:
            manager = WorkspaceManager.get_instance()
            session = await manager.get_session_for_workspace(workspace_id)
            sandbox = getattr(session, "sandbox", None)
            if sandbox:
                norm, error = sandbox.validate_and_normalize_path(path)
                if error:
                    raise HTTPException(status_code=403, detail=error)

                content = await sandbox.adownload_file_bytes(norm)
                if content is None:
                    raise HTTPException(status_code=404, detail="File not found")

                from src.server.app.workspace_files import _to_client_path
                client_path = _to_client_path(sandbox, norm)
                if _is_always_hidden_path(client_path):
                    raise HTTPException(status_code=404, detail="File not found")

                filename = client_path.split("/")[-1] if client_path else "download"
                mime, _ = mimetypes.guess_type(filename)

                if mime and mime.startswith("text/"):
                    content = get_redactor().redact_bytes(content, vault_secrets=vault_secrets)

                return StreamingResponse(
                    iter([content]),
                    media_type=mime or "application/octet-stream",
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'},
                )
        except HTTPException:
            raise
        except Exception:
            logger.debug(f"Sandbox not available for shared file download in workspace {workspace_id}")

    raise HTTPException(status_code=404, detail="File not found")
