"""
Public Share Router — Unauthenticated endpoints for shared thread access.

All endpoints use an opaque share_token instead of thread/workspace IDs.
No auth required. workspace_id is resolved server-side and never exposed.

Endpoints:
- GET /api/v1/public/shared/{share_token}          — Thread metadata
- GET /api/v1/public/shared/{share_token}/replay    — SSE conversation replay
- GET /api/v1/public/shared/{share_token}/files     — File listing (requires allow_files)
- GET /api/v1/public/shared/{share_token}/files/read     — Read file content (requires allow_files)
- GET /api/v1/public/shared/{share_token}/files/serve/{path} — Serve file inline with sandboxed CSP (requires allow_files)
- GET /api/v1/public/shared/{share_token}/files/download — Download raw file (requires allow_download)
"""

import asyncio
import json
import logging
import mimetypes
from typing import Any

from fastapi import APIRouter, HTTPException, Path, Query, Request
from fastapi.responses import Response, StreamingResponse

from src.observability import observe_replay_stream
from src.server.utils.http_headers import content_disposition
from src.server.utils.secret_redactor import get_redactor, get_vault_secrets_for_redaction

from src.server.database.conversation import (
    get_thread_by_share_token,
    get_queries_for_thread,
    get_responses_for_thread,
)
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.app.workspace_files import (
    _get_work_dir,
    _has_traversal,
    _is_always_hidden_path,
    _is_hidden_path,
    _is_system_path,
    _is_text_content_type,
    _is_utf8,
    _normalize_requested_path,
    _is_binary,
    render_workspace_file_pdf,
    serve_workspace_file,
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


def _wants_html(request: Request) -> bool:
    """True when the caller is a browser/iframe (Accept includes text/html), so a
    failed serve should render a page instead of raw JSON. API clients (Accept
    ``*/*`` etc.) keep the JSON error."""
    return "text/html" in request.headers.get("accept", "").lower()


def _prefers_chinese(request: Request) -> bool:
    """Whether the browser's top Accept-Language tag is Chinese."""
    primary = request.headers.get("accept-language", "").split(",")[0].strip().lower()
    return primary.startswith("zh")


def _share_unavailable_page(status_code: int, *, chinese: bool) -> str:
    """Self-contained, theme-aware HTML page shown when a browser opens a shared
    report link that has been revoked (404) or lacks file access (403)."""
    if chinese:
        lang = "zh-CN"
        heading = "此分享报告不可用"
        detail = (
            "创建者尚未为此链接开启文件访问权限。"
            if status_code == 403
            else "该链接可能已被创建者关闭，或报告已不存在。"
        )
        link_text = "前往 LangAlpha"
    else:
        lang = "en"
        heading = "This shared report isn’t available"
        detail = (
            "The owner hasn’t enabled file access for this link."
            if status_code == 403
            else "The link may have been turned off by its owner, or the report no longer exists."
        )
        link_text = "Go to LangAlpha"
    return f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light dark">
<meta name="robots" content="noindex">
<title>{heading}</title>
<style>
  :root {{
    --bg: #F3EEE8; --bg-glow: rgba(55, 82, 139, 0.06);
    --card: #FFFCF9; --border: rgba(0, 0, 0, 0.08);
    --text: #2D2B28; --muted: #7A756F; --accent: #37528B;
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{
      --bg: #000000; --bg-glow: rgba(65, 97, 164, 0.12);
      --card: #0A0A0A; --border: rgba(255, 255, 255, 0.08);
      --text: #FFFFFF; --muted: #999999; --accent: #4161A4;
    }}
  }}
  * {{ box-sizing: border-box; }}
  html, body {{ height: 100%; margin: 0; }}
  body {{
    display: flex; align-items: center; justify-content: center;
    min-height: 100%; padding: 24px;
    background: radial-gradient(circle at 50% 32%, var(--bg-glow), transparent 60%), var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    line-height: 1.6;
  }}
  .card {{
    max-width: 30rem; width: 100%; text-align: center;
    background: var(--card); border: 1px solid var(--border);
    border-radius: 16px; padding: 40px 32px;
  }}
  .badge {{
    font-size: 13px; font-weight: 600; letter-spacing: .01em;
    color: var(--muted); margin-bottom: 20px;
  }}
  h1 {{ font-size: 20px; font-weight: 600; margin: 0 0 10px; }}
  p {{ font-size: 14px; color: var(--muted); margin: 0 0 24px; }}
  a {{ font-size: 14px; font-weight: 500; color: var(--accent); text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
  <div class="card">
    <div class="badge">LangAlpha</div>
    <h1>{heading}</h1>
    <p>{detail}</p>
    <a href="/">{link_text}</a>
  </div>
</body>
</html>"""


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
        observe_replay_stream(event_generator(), source="public"),
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

    # Reject `..` before it reaches the sandbox path validator, which only
    # prefix-checks the work dir and does not resolve `..` — so an unresolved
    # `../../etc/passwd` would otherwise read outside the workspace on a live
    # sandbox. Mirrors the serve-core guard (workspace_files._has_traversal).
    if _has_traversal(path):
        raise HTTPException(status_code=404, detail="File not found")

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

    normalized_path = _normalize_requested_path(path, _get_work_dir())
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

    # See list_shared_files: reject `..` before the sandbox validator, which
    # does not resolve it.
    if _has_traversal(path):
        raise HTTPException(status_code=404, detail="File not found")

    from src.server.database.workspace import get_workspace as db_get_workspace
    from src.server.services.persistence.file import FilePersistenceService
    from src.server.services.workspace_manager import WorkspaceManager

    workspace = await db_get_workspace(workspace_id)
    if not workspace:
        raise HTTPException(status_code=404, detail="Workspace not found")

    normalized_path = _normalize_requested_path(path, _get_work_dir())
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

    # See list_shared_files: reject `..` before the sandbox validator, which
    # does not resolve it.
    if _has_traversal(path):
        raise HTTPException(status_code=404, detail="File not found")

    from src.server.database.workspace import get_workspace as db_get_workspace
    from src.server.services.persistence.file import FilePersistenceService
    from src.server.services.workspace_manager import WorkspaceManager

    workspace = await db_get_workspace(workspace_id)
    if not workspace:
        raise HTTPException(status_code=404, detail="Workspace not found")

    normalized_path = _normalize_requested_path(path, _get_work_dir())
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

        if _is_text_content_type(mime) or _is_utf8(content):
            content = get_redactor().redact_bytes(content, vault_secrets=vault_secrets)

        return StreamingResponse(
            iter([content]),
            media_type=mime,
            headers={"Content-Disposition": content_disposition(filename)},
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

                if _is_text_content_type(mime or "") or _is_utf8(content):
                    content = get_redactor().redact_bytes(content, vault_secrets=vault_secrets)

                return StreamingResponse(
                    iter([content]),
                    media_type=mime or "application/octet-stream",
                    headers={"Content-Disposition": content_disposition(filename)},
                )
        except HTTPException:
            raise
        except Exception:
            logger.debug(f"Sandbox not available for shared file download in workspace {workspace_id}")

    raise HTTPException(status_code=404, detail="File not found")


@router.get("/shared/{share_token}/files/serve/{path:path}")
async def serve_shared_file(
    request: Request,
    share_token: str,
    path: str = Path(..., description="File path within the shared workspace."),
    inject: str | None = Query(None, description="Set to 'theme' to splice theme-sync into HTML."),
    format: str | None = Query(None, description="Set to 'pdf' to render HTML as a PDF."),
    scale: float | None = Query(
        None, ge=0.5, le=2.0, description="PDF only: render scale (0.5–2.0)."
    ),
    page_numbers: bool = Query(
        False, description="PDF only: draw an 'N / total' footer in the page margin."
    ),
    branding: bool = Query(
        True, description="PDF only: stamp 'LangAlpha · <date>' in the footer."
    ),
) -> Response:
    """Serve a shared workspace file inline with a sandboxed CSP. Requires allow_files.

    Path-style so a served document's relative subresources (``charts/x.png``)
    resolve under the same token prefix. Reuses the workspace file-serving core
    (MIME / traversal / redaction / DB-fallback / theme injection); the
    workspace UUID is resolved server-side and never appears in the URL.
    ``?format=pdf`` renders HTML files via server-side Chromium over the same
    internal wsfiles URL (the public URL never matters internally).

    Gated on ``allow_files`` only, matching ``read_shared_file``: in this share
    model ``allow_files`` already grants byte access to file content, and
    ``allow_download`` gates the explicit download affordance, not raw content
    reachability. So serving (and PDF export) need only ``allow_files``.
    """
    try:
        _, workspace_id = await _get_shared_workspace_id(share_token, require_files=True)

        workspace = await db_get_workspace(workspace_id)
        if not workspace:
            raise HTTPException(status_code=404, detail="Not found")

        if format == "pdf":
            return await render_workspace_file_pdf(
                workspace_id,
                path,
                workspace=workspace,
                scale=scale,
                page_numbers=page_numbers,
                branding=branding,
            )

        return await serve_workspace_file(
            workspace_id,
            path,
            inject_theme=(inject == "theme"),
            workspace=workspace,
        )
    except HTTPException as exc:
        # A browser/iframe opening a revoked (404) or forbidden (403) shared link
        # should see a branded page, not raw JSON. Other statuses and API clients
        # (Accept without text/html) keep the default JSON error.
        if exc.status_code in (403, 404) and _wants_html(request):
            return Response(
                content=_share_unavailable_page(exc.status_code, chinese=_prefers_chinese(request)),
                status_code=exc.status_code,
                media_type="text/html; charset=utf-8",
                headers={"Cache-Control": "no-store", "X-Robots-Tag": "noindex"},
            )
        raise
