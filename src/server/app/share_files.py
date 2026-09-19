"""The file routes of a shared thread: list, resolve, read, download and serve.

Five routes, one resolution of the token (``share_access.resolve_shared_files``)
and one rule about where a file's bytes come from (``_shared_file_bytes``). They
used to disagree on that rule, which is how the same token could answer the
iframe with a report the panel beside it had never heard of.

Registered onto the public router in ``public.py``; the paths below are
relative to ``/api/v1/public``.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from fastapi import APIRouter, HTTPException, Path, Query, Request
from fastapi.responses import Response, StreamingResponse

from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError
from src.server.app.share_access import SharedFileTarget, resolve_shared_files
from src.server.app.share_pages import unavailable_response
from src.server.app.workspace_files._containment import (
    contained_listing_path,
    contained_relative_path,
    contained_sandbox_path,
    is_within,
)
from src.server.app.workspace_files._shared import (
    DEFAULT_READ_LIMIT_LINES,
    _is_binary,
    _is_flash_workspace,
    _is_text_content_type,
    _is_utf8,
    _to_client_path,
)
from src.server.app.workspace_files.file_refs import (
    ResolveFileRefRequest,
    clean_candidates,
    clean_path,
    resolve_file_ref,
)
from src.server.app.workspace_files.serve import (
    _has_traversal,
    render_workspace_file_pdf,
    serve_workspace_file,
    warm_sandbox,
    warm_sandbox_bytes,
)
from src.server.services.persistence.file import FilePersistenceService
from src.server.services.persistence.resolve import resolve_file_bytes_or_none
from src.server.utils.error_sanitization import single_line
from src.server.utils.http_headers import content_disposition
from src.server.utils.secret_redactor import (
    get_redactor,
    get_vault_secrets_for_redaction,
)
from src.utils.mime import resolve_content_type

logger = logging.getLogger(__name__)

share_files_router = APIRouter(tags=["Public Sharing"])

# Every sandbox failure on these routes collapses into the same 404 an absent
# path gets. They are unauthenticated, so a 503 would confirm that a guessed
# token resolved to a real workspace. Warning, not debug: the response hides
# the cause on purpose, which makes the log line the only place it survives,
# and a debug line is dropped at the default level.
_SANDBOX_MISS = "Sandbox not available for shared {what} in workspace {ws}: {err}"

_NOT_FOUND = "File not found"


def _log_sandbox_miss(what: str, workspace_id: str, err: Exception) -> None:
    logger.warning(
        _SANDBOX_MISS.format(what=what, ws=workspace_id, err=single_line(str(err)))
    )


def _mime_for(client_path: str, row_mime: str | None) -> str:
    """Content type from the pinned extension map, row mime as the fallback.

    The same rule the serving core uses, so a file does not change type
    depending on which of these routes returned it.
    """
    mime = resolve_content_type(client_path)
    if mime == "application/octet-stream" and row_mime:
        return row_mime
    return mime


@dataclass(frozen=True)
class SharedBytes:
    """One file as a share route answers it."""

    path: str
    content: bytes
    mime: str
    source: str


async def _shared_file_bytes(
    target: SharedFileTarget, normalized_path: str
) -> SharedBytes | None:
    """The bytes behind an in-scope path: live when the sandbox is warm, else the manifest.

    A warm sandbox is the answer, including when its answer is "no such file":
    falling through to the manifest there would serve a path the live tree has
    deleted, and would serve one the visibility gate just refused, since a
    refusal and a miss reach this function as the same ``None``.

    Never wakes a sandbox. These routes carry no credential but the token, and
    provisioning from a URL alone is someone else's bill.
    """
    workspace_id = target.workspace_id
    sandbox = warm_sandbox(target.workspace, workspace_id)
    if sandbox is not None:
        try:
            resolved = await warm_sandbox_bytes(
                sandbox,
                normalized_path,
                work_dir=target.work_dir,
                visible=target.visible,
            )
        except Exception as e:
            _log_sandbox_miss("file", workspace_id, e)
        else:
            if resolved is None:
                return None
            client_path, content = resolved
            return SharedBytes(
                client_path, content, _mime_for(client_path, None), "sandbox"
            )

    record = await FilePersistenceService.get_file_content(
        workspace_id, normalized_path
    )
    if not record:
        return None
    content = await resolve_file_bytes_or_none(
        record,
        user_id=target.workspace["user_id"],
        context=f"reading shared file in workspace {workspace_id}",
    )
    # The resolver answers None for a storage failure and for a row with no
    # content alike, and this route gives both the 404 an absent path gets: the
    # caller holds a share token, so "the bytes exist but are unavailable"
    # would confirm the workspace and the path.
    if content is None:
        return None
    return SharedBytes(
        normalized_path,
        content,
        _mime_for(normalized_path, record.get("mime_type")),
        "database",
    )


async def _live_listing(
    sandbox, target: SharedFileTarget, normalized_path: str
) -> list[str]:
    """Visible paths under a directory, from the sandbox that holds them."""
    work_dir = target.work_dir
    # The contained directory, never the raw one: the sandbox validator
    # prefix-tests the computer root, which holds every sibling workspace and
    # the machine's shared /tmp, and the canonical form is what a symlinked
    # directory is judged on.
    glob_root = f"{work_dir}/{normalized_path}" if normalized_path else work_dir
    glob_root = await contained_sandbox_path(sandbox, glob_root, work_dir=work_dir)
    if glob_root is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    absolute_paths = await sandbox.aglob_files("**/*", path=glob_root)
    files = []
    for absolute in absolute_paths:
        if not is_within(work_dir, absolute):
            continue
        client_path = _to_client_path(sandbox, absolute, work_dir)
        if target.visible(client_path):
            files.append(client_path)
    return files


async def _visible_listing(
    target: SharedFileTarget, normalized_path: str
) -> tuple[list[str], str]:
    """Visible paths under a directory and where they came from.

    A warm sandbox answers first; anything short of a route error there falls
    back to the manifest, which is what an unauthenticated caller must never be
    able to tell apart from a sandbox that was never warm.
    """
    sandbox = warm_sandbox(target.workspace, target.workspace_id)
    if sandbox is not None:
        try:
            return await _live_listing(sandbox, target, normalized_path), "sandbox"
        except HTTPException:
            raise
        except Exception as e:
            _log_sandbox_miss("files", target.workspace_id, e)
    return await _stored_listing(target, normalized_path), "database"


async def _stored_listing(target: SharedFileTarget, normalized_path: str) -> list[str]:
    """Visible paths under a directory, from the manifest."""
    file_tree = await FilePersistenceService.get_file_tree(target.workspace_id)
    return [
        f["path"]
        for f in file_tree
        if (
            not normalized_path
            or f["path"] == normalized_path
            or f["path"].startswith(f"{normalized_path}/")
        )
        and target.visible(f["path"])
    ]


@share_files_router.get("/shared/{share_token}/files")
async def list_shared_files(
    share_token: str,
    path: str = Query(".", description="Directory to list."),
):
    """List files in a shared thread's workspace. Requires allow_files permission."""
    # Reject `..` before it reaches the sandbox path validator, which only
    # prefix-checks the work dir and does not resolve `..`, so an unresolved
    # `../../etc/passwd` would otherwise read outside the workspace on a live
    # sandbox. Mirrors the serve-core guard (workspace_files._has_traversal).
    # First, so a hostile path is refused without a lookup telling the caller
    # anything about the token.
    if _has_traversal(path):
        raise HTTPException(status_code=404, detail=_NOT_FOUND)

    target = await resolve_shared_files(share_token, require_files=True)
    if _is_flash_workspace(target.workspace):
        return {"files": [], "source": "none"}

    normalized_path = contained_listing_path(path, target.work_dir)
    if normalized_path is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)

    files, source = await _visible_listing(target, normalized_path)
    return {"path": path, "files": files, "source": source}


@share_files_router.post("/shared/{share_token}/files/resolve")
async def resolve_shared_file(share_token: str, body: ResolveFileRefRequest):
    """Resolve a file reference inside a shared thread's workspace.

    Matches against the same listing the shared file panel browses, so it never
    wakes a sandbox and never finds a file that listing would hide, the share
    scope included.
    """
    target = await resolve_shared_files(share_token, require_files=True)
    if _is_flash_workspace(target.workspace):
        return {"status": "unavailable", "reason": "flash_workspace", "matches": []}

    candidates = clean_candidates(body.candidates, target.work_dir)
    if not candidates:
        raise HTTPException(status_code=400, detail="A file reference is required")
    recent_writes = [
        p for p in (clean_path(w, target.work_dir) for w in body.recent_writes) if p
    ]
    files, source = await _visible_listing(target, "")
    result = resolve_file_ref(candidates, files, recent_writes)
    return {**result, "source": source}


@share_files_router.get("/shared/{share_token}/files/read")
async def read_shared_file(
    share_token: str,
    path: str = Query(..., description="File path to read."),
    offset: int = Query(0, ge=0, description="Line offset."),
    limit: int = Query(
        DEFAULT_READ_LIMIT_LINES,
        ge=1,
        le=DEFAULT_READ_LIMIT_LINES,
        description="Max lines.",
    ),
):
    """Read a text file from a shared thread's workspace. Requires allow_files permission."""
    # See list_shared_files: reject `..` before the sandbox validator, which
    # does not resolve it.
    if _has_traversal(path):
        raise HTTPException(status_code=404, detail=_NOT_FOUND)

    target = await resolve_shared_files(share_token, require_files=True)
    normalized_path = contained_relative_path(path, target.work_dir)
    if normalized_path is None or not target.visible(normalized_path):
        raise HTTPException(status_code=404, detail=_NOT_FOUND)

    vault_secrets, resolved = await asyncio.gather(
        get_vault_secrets_for_redaction(target.workspace_id),
        _shared_file_bytes(target, normalized_path),
    )
    if resolved is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    if _is_binary(resolved.path):
        raise HTTPException(
            status_code=415, detail="Cannot read binary file as text."
        )
    try:
        text = resolved.content.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=415, detail="File appears to be binary.")

    text = get_redactor().redact(text, vault_secrets=vault_secrets)
    lines = text.splitlines()
    return {
        "path": resolved.path,
        "offset": offset,
        "limit": limit,
        "content": "\n".join(lines[offset : offset + limit]),
        "mime": resolved.mime or "text/plain",
        "truncated": len(lines) > offset + limit,
        "source": resolved.source,
    }


@share_files_router.get("/shared/{share_token}/files/download")
async def download_shared_file(
    share_token: str,
    path: str = Query(..., description="File path to download."),
):
    """Download a raw file from a shared thread's workspace. Requires allow_download permission."""
    # See list_shared_files: reject `..` before the sandbox validator, which
    # does not resolve it.
    if _has_traversal(path):
        raise HTTPException(status_code=404, detail=_NOT_FOUND)

    target = await resolve_shared_files(share_token, require_download=True)
    normalized_path = contained_relative_path(path, target.work_dir)
    if normalized_path is None or not target.visible(normalized_path):
        raise HTTPException(status_code=404, detail=_NOT_FOUND)

    vault_secrets, resolved = await asyncio.gather(
        get_vault_secrets_for_redaction(target.workspace_id),
        _shared_file_bytes(target, normalized_path),
    )
    if resolved is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)

    content = resolved.content
    # Redact any UTF-8-decodable body, not just declared text types: a secret
    # written to a mis-named file would otherwise leave verbatim.
    if _is_text_content_type(resolved.mime) or _is_utf8(content):
        content = get_redactor().redact_bytes(content, vault_secrets=vault_secrets)

    filename = resolved.path.rsplit("/", 1)[-1] or "download"
    return StreamingResponse(
        iter([content]),
        media_type=resolved.mime or "application/octet-stream",
        headers={"Content-Disposition": content_disposition(filename)},
    )


@share_files_router.get("/shared/{share_token}/files/serve/{path:path}")
async def serve_shared_file(
    request: Request,
    share_token: str,
    path: str = Path(..., description="File path within the shared workspace."),
    inject: str | None = Query(
        None, description="Set to 'theme' to splice theme-sync into HTML."
    ),
    format: str | None = Query(None, description="Set to 'pdf' to render HTML as a PDF."),
    scale: float | None = Query(
        None, ge=0.5, le=2.0, description="PDF only: render scale (0.5-2.0)."
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
    (MIME / traversal / redaction / fallback / theme injection) with the token's
    own reach as the visibility gate, so the core judges the path a sandbox read
    resolved to and not the one the URL asked for. The workspace UUID is
    resolved server-side and never appears in the URL.

    ``?format=pdf`` renders HTML through server-side Chromium, and renders it
    through *this* route rather than the workspace one: the browser fetches
    subresources with no credential of its own, so whatever route it fetches
    them from is the only thing standing between a scoped token and the rest of
    the workspace.

    Gated on ``allow_files`` only, matching ``read_shared_file``: in this share
    model ``allow_files`` already grants byte access to file content, and
    ``allow_download`` gates the explicit download affordance, not raw content
    reachability. So serving (and PDF export) need only ``allow_files``.
    """
    try:
        target = await resolve_shared_files(share_token, require_files=True)

        if format == "pdf":
            return await render_workspace_file_pdf(
                target.workspace_id,
                path,
                workspace=target.workspace,
                scale=scale,
                page_numbers=page_numbers,
                branding=branding,
                visible=target.visible,
                serve_base=f"/api/v1/public/shared/{share_token}/files/serve/",
            )

        return await serve_workspace_file(
            target.workspace_id,
            path,
            inject_theme=(inject == "theme"),
            workspace=target.workspace,
            visible=target.visible,
        )
    except (SandboxGoneError, SandboxTransientError):
        # This route is unauthenticated, so it must never distinguish "sandbox
        # down" from "no such file", a 503 would confirm that a guessed
        # workspace UUID is real. Today serve.py absorbs these before they get
        # here; this keeps the 404 posture from depending on that.
        page = unavailable_response(request, 404)
        if page is not None:
            return page
        raise HTTPException(status_code=404, detail=_NOT_FOUND) from None
    except HTTPException as exc:
        # A browser/iframe opening a revoked (404) or forbidden (403) shared link
        # should see a branded page, not raw JSON. Other statuses and API clients
        # (Accept without text/html) keep the default JSON error.
        page = unavailable_response(request, exc.status_code)
        if page is not None:
            return page
        raise
