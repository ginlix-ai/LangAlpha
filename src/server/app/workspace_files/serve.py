"""Unauthenticated path-style workspace file serving (`/api/v1/wsfiles/...`)."""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query

from src.config.env import PDF_RENDER_INTERNAL_BASE
from src.server.utils.http_headers import content_disposition
from fastapi.responses import Response

from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.services.workspace_manager import WorkspaceManager
from src.server.services.persistence.file import FilePersistenceService
from src.server.utils.secret_redactor import get_redactor, get_vault_secrets_for_redaction
from src.utils.mime import resolve_content_type

from ._shared import (
    _acquire_sandbox,
    _is_text_content_type,
    _is_utf8,
    _get_work_dir,
    _is_flash_workspace,
    _is_serve_blocked_path,
    _normalize_requested_path,
    _record_fs_bytes,
    _to_client_path,
)

logger = logging.getLogger(__name__)



# ---------------------------------------------------------------------------
# Unauthenticated path-style file serving (`/api/v1/wsfiles/...`)
# ---------------------------------------------------------------------------
#
# Gives `.html` deliverables true served semantics: a document served at
# `/wsfiles/{ws}/results/report.html` can reference `charts/foo.png` and the
# browser resolves it relatively to `/wsfiles/{ws}/results/charts/foo.png`.
# The workspace UUID (128-bit) is the access credential, mirroring the
# `preview_redirect_router` posture: uniform 404 for missing/unauthorized,
# and never wake a stopped sandbox (denial-of-wallet protection).
#
# This is an internal serving mechanism, NOT a sharing primitive — the
# workspace UUID grants read access to every file in the workspace.
# User-facing sharing goes through permission-scoped thread-share tokens.

wsfiles_router = APIRouter(prefix="/api/v1", tags=["Workspace File Serving"])

# Short private cache: HTML reports and their assets are effectively immutable
# for a turn, so up to 60s of staleness on reload (until the next agent update
# is picked up) is an acceptable trade for far fewer sandbox/DB reads. The
# workspace UUID is a bearer credential, so we never allow shared/public caches
# to retain the bytes.
_WSFILES_CACHE_CONTROL = "private, max-age=60"

# Content-Security-Policy for served reports. Two jobs:
#   1. `sandbox allow-scripts` forces an opaque origin even though the iframe
#      loads via `src=`, so agent/prompt-injected HTML can never reach app
#      cookies/localStorage.
#   2. The source directives cap egress to the html-report skill's CDN
#      allowlist. `connect-src 'none'` is the load-bearing block: no
#      fetch/XHR/beacon/websocket, so a prompt-injected report cannot exfiltrate
#      its own contents. The skill embeds data inline, so this costs nothing.
# `'self'` keeps relative subresources (charts/foo.png, app.js) working;
# `'unsafe-inline'` is required because reports inline their JS/CSS and the
# server splices an inline theme-sync script for `?inject=theme`. Google Fonts
# stays allowed for the CJK web-font path (Noto Sans SC/JP/KR -> tofu without it).
_WSFILES_CSP = (
    "sandbox allow-scripts; "
    "default-src 'none'; "
    "script-src 'self' 'unsafe-inline' "
    "https://cdnjs.cloudflare.com https://cdn.jsdelivr.net "
    "https://unpkg.com https://esm.sh; "
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com "
    "https://cdnjs.cloudflare.com https://cdn.jsdelivr.net https://unpkg.com; "
    "img-src 'self' data: blob:; "
    "font-src 'self' data: https://fonts.gstatic.com https://cdnjs.cloudflare.com; "
    "connect-src 'none'; "
    "frame-src 'none'; "
    "base-uri 'none'; "
    "form-action 'none'"
)

# Theme-sync script spliced after <head> when `?inject=theme` is set. Listens
# for `widget:themeUpdate` postMessages and applies the `--color-*` custom
# properties to :root via a dedicated style element. The payload field (`css`,
# a `--color-x: value;` block) and message type match the inline-widget theme
# protocol the parent already speaks (useHtmlSandbox.pushTheme).
_THEME_INJECTION = (
    '<meta name="color-scheme" content="light dark">'
    "<script>(function(){"
    "function apply(css){"
    "var id='__wsfiles_theme__';var s=document.getElementById(id);"
    "if(!s){s=document.createElement('style');s.id=id;"
    "(document.head||document.documentElement).appendChild(s);}"
    "s.textContent=':root{\\n'+css+'\\n}';}"
    "window.addEventListener('message',function(e){"
    "var d=e&&e.data;"
    "if(!d||d.type!=='widget:themeUpdate'||!d.css)return;"
    "apply(d.css);});})();</script>"
)


def _guess_content_type(path: str) -> str:
    """Resolve a Content-Type for a served file via the canonical pinned map."""
    return resolve_content_type(path)


def _is_html_content_type(content_type: str) -> bool:
    return content_type.split(";", 1)[0].strip().lower() in ("text/html", "text/htm")


def _inject_theme_into_html(html: str) -> str:
    """Splice the theme-sync snippet immediately after <head> (case-insensitive).

    Falls back to prepending when no <head> tag is present so even fragment
    documents still receive the listener.
    """
    lower = html.lower()
    idx = lower.find("<head>")
    if idx != -1:
        insert_at = idx + len("<head>")
        return html[:insert_at] + _THEME_INJECTION + html[insert_at:]
    # No literal <head> — try <head ...> with attributes.
    match = re.search(r"<head\b[^>]*>", html, re.IGNORECASE)
    if match:
        insert_at = match.end()
        return html[:insert_at] + _THEME_INJECTION + html[insert_at:]
    return _THEME_INJECTION + html


def _has_traversal(path: str) -> bool:
    """Reject `..` segments before they reach path resolution."""
    return ".." in (path or "").replace("\\", "/").split("/")


async def _db_fallback_bytes(
    workspace_id: str, normalized_path: str, extension_mime: str
) -> tuple[bytes, str] | None:
    """Read a file's bytes from the persisted DB record (no sandbox I/O)."""
    file_record = await FilePersistenceService.get_file_content(
        workspace_id, normalized_path
    )
    if not file_record:
        return None
    if file_record.get("is_binary") and file_record.get("content_binary") is not None:
        content = file_record["content_binary"]
        if isinstance(content, memoryview):
            content = bytes(content)
    elif file_record.get("content_text") is not None:
        content = file_record["content_text"].encode("utf-8")
    else:
        return None
    # Extension is the authority for known web types; fall back to the
    # DB-stored mime only when the extension is unrecognized.
    if extension_mime == "application/octet-stream" and file_record.get("mime_type"):
        return content, file_record["mime_type"]
    return content, extension_mime


async def _resolve_serve_bytes(
    workspace: dict[str, Any], workspace_id: str, normalized_path: str
) -> tuple[bytes, str] | None:
    """Resolve raw bytes + content type for a file, sandbox-first with DB fallback.

    Returns ``(content, content_type)`` or ``None`` when the file is missing.
    Live bytes are read only when the sandbox is already warm in this worker
    (``has_ready_session``); otherwise — including a stale ``running`` DB row
    whose Daytona sandbox has auto-stopped — this falls back to the persisted
    DB record. This keeps the unauthenticated route from ever waking or
    recovering a sandbox (denial-of-wallet) from a UUID-only request.
    """
    extension_mime = _guess_content_type(normalized_path)

    # Live read only from an already-warm sandbox; short-circuit so the manager
    # singleton is consulted only on the running path. A stale 'running' DB row
    # whose Daytona sandbox auto-stopped has no warm session → DB fallback.
    status = workspace.get("status")
    warm = (
        status == "running"
        and WorkspaceManager.get_instance().has_ready_session(workspace_id)
    )
    if not warm:
        return await _db_fallback_bytes(workspace_id, normalized_path, extension_mime)

    # Warm sandbox — read live bytes without any Daytona start/reconnect. If
    # the session died between has_ready_session() above and here (TOCTOU),
    # _acquire_sandbox raises 503; absorb it and fall back to the DB record so
    # the route keeps its uniform-404 posture instead of leaking a 503 that
    # would confirm the workspace UUID is valid.
    try:
        sandbox = await _acquire_sandbox(workspace_id, workspace.get("user_id") or "")
    except HTTPException:
        return await _db_fallback_bytes(workspace_id, normalized_path, extension_mime)
    candidate, error = sandbox.validate_and_normalize_path(normalized_path)
    if error:
        return None
    try:
        content = await sandbox.adownload_file_bytes(candidate)
    except RuntimeError:
        return None
    if content is None:
        return None
    client_path = _to_client_path(sandbox, candidate)
    if _is_serve_blocked_path(client_path):
        return None
    return content, extension_mime


async def serve_workspace_file(
    workspace_id: str,
    path: str,
    *,
    inject_theme: bool,
    workspace: dict[str, Any] | None = None,
) -> Response:
    """Serve one workspace file inline with a sandboxed CSP and optional theming.

    Resolves the file (running sandbox first, DB fallback for stopped
    workspaces), picks the Content-Type, redacts vault secrets from text
    bodies, and emits the sandboxed, egress-capped ``_WSFILES_CSP`` on every
    response. When ``inject_theme`` is set and the body is HTML, a small
    theme-sync ``<script>`` is spliced after ``<head>``; otherwise the bytes
    are served faithfully. Missing/unknown/traversal inputs all raise a uniform
    404 so the endpoint never reveals which check failed.

    ``workspace`` may be passed pre-resolved (e.g. by a share-token route) to
    reuse this core with a different credential resolver; otherwise the
    workspace is looked up by UUID.
    """
    if _has_traversal(path):
        raise HTTPException(status_code=404, detail="Not found")

    if workspace is None:
        try:
            workspace = await db_get_workspace(workspace_id)
        except Exception:
            raise HTTPException(
                status_code=404, detail="Not found"
            ) from None
    if not workspace or _is_flash_workspace(workspace):
        raise HTTPException(status_code=404, detail="Not found")

    work_dir = _get_work_dir()
    normalized_path = _normalize_requested_path(path, work_dir)
    if not normalized_path or _is_serve_blocked_path(normalized_path):
        raise HTTPException(status_code=404, detail="Not found")

    resolved = await _resolve_serve_bytes(workspace, workspace_id, normalized_path)
    if resolved is None:
        raise HTTPException(status_code=404, detail="Not found")
    content, content_type = resolved

    # Redact vault secrets from any UTF-8-decodable body, not just declared
    # text MIME types — otherwise a secret written to a mis-named file (e.g.
    # secret.png) would bypass redaction on this unauthenticated, shareable
    # route. Genuine binary fails to decode and is served verbatim, so we also
    # skip the per-asset vault fetch for it.
    if _is_text_content_type(content_type) or _is_utf8(content):
        vault_secrets = await get_vault_secrets_for_redaction(workspace_id)
        content = get_redactor().redact_bytes(content, vault_secrets=vault_secrets)

    if inject_theme and _is_html_content_type(content_type):
        # Only inject when the body is valid UTF-8. A non-UTF-8 HTML document
        # (GBK/Shift-JIS, preserved losslessly by the latin-1 redaction fallback)
        # would be corrupted by an errors="replace" decode-then-reencode, so
        # serve it byte-faithful instead and skip theme sync for that rare case.
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            pass
        else:
            content = _inject_theme_into_html(text).encode("utf-8")

    headers = {
        "Content-Security-Policy": _WSFILES_CSP,
        "Cache-Control": _WSFILES_CACHE_CONTROL,
        "Content-Disposition": content_disposition(
            normalized_path.rsplit("/", 1)[-1] or "file", disposition="inline"
        ),
        "X-Content-Type-Options": "nosniff",
    }
    _record_fs_bytes("serve", len(content))
    return Response(content=content, media_type=content_type, headers=headers)


async def render_workspace_file_pdf(
    workspace_id: str,
    path: str,
    *,
    workspace: dict[str, Any] | None = None,
    scale: float | None = None,
    page_numbers: bool = False,
    branding: bool = True,
) -> Response:
    """Render a workspace HTML file to PDF via headless Chromium.

    Pre-validates the file exists and is HTML (same uniform 404 posture as the
    inline serve, so chromium never spins on garbage), then renders the byte-
    faithful internal wsfiles URL (no theme injection) under an SSRF-gated
    browser. Renderer failures map to 501/504/500 — intentionally NOT 404,
    since the file exists and only the converter failed.
    """
    if _has_traversal(path):
        raise HTTPException(status_code=404, detail="Not found")

    if workspace is None:
        try:
            workspace = await db_get_workspace(workspace_id)
        except Exception:
            raise HTTPException(status_code=404, detail="Not found") from None
    if not workspace or _is_flash_workspace(workspace):
        raise HTTPException(status_code=404, detail="Not found")

    work_dir = _get_work_dir()
    normalized_path = _normalize_requested_path(path, work_dir)
    if not normalized_path or _is_serve_blocked_path(normalized_path):
        raise HTTPException(status_code=404, detail="Not found")

    # Cheap pre-validation: resolve bytes + content type and require HTML.
    resolved = await _resolve_serve_bytes(workspace, workspace_id, normalized_path)
    if resolved is None:
        raise HTTPException(status_code=404, detail="Not found")
    _content, content_type = resolved
    if not _is_html_content_type(content_type):
        raise HTTPException(status_code=404, detail="Not found")

    from src.server.services import pdf_render

    base = PDF_RENDER_INTERNAL_BASE.rstrip("/")
    # Percent-encode the path (UTF-8) so metacharacters (#, ?, space) and
    # non-ASCII (CJK) survive into headless Chromium; keep `/` so the path
    # structure stays intact. The wsfiles endpoint decodes it back to unicode.
    encoded_path = quote(normalized_path, safe="/")
    internal_url = f"{base}/api/v1/wsfiles/{workspace_id}/{encoded_path}"
    serve_prefix = f"{base}/api/v1/wsfiles/{workspace_id}/"
    try:
        pdf_bytes = await pdf_render.render_workspace_pdf(
            internal_url,
            workspace_serve_prefix=serve_prefix,
            scale=scale,
            page_numbers=page_numbers,
            branding=branding,
        )
    except pdf_render.PdfRenderUnavailable:
        raise HTTPException(status_code=501, detail="PDF rendering not available")
    except pdf_render.PdfRenderTimeout:
        raise HTTPException(status_code=504, detail="PDF rendering timed out")
    except pdf_render.PdfRenderError:
        logger.exception("PDF render failed for workspace file")
        raise HTTPException(status_code=500, detail="PDF rendering failed")

    stem = normalized_path.rsplit("/", 1)[-1].rsplit(".", 1)[0] or "document"
    headers = {
        "Content-Disposition": content_disposition(
            f"{stem}.pdf", disposition="attachment"
        ),
        "Cache-Control": _WSFILES_CACHE_CONTROL,
    }
    _record_fs_bytes("pdf", len(pdf_bytes))
    return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)


@wsfiles_router.get("/wsfiles/{workspace_id}/{path:path}")
async def serve_workspace_file_endpoint(
    workspace_id: str,
    path: str,
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
    """Serve a workspace file by path with sandboxed CSP (unauthenticated).

    Workspace UUID is the credential; uniform 404 for unknown workspace,
    missing file, or traversal. ``?inject=theme`` adds theme-sync to HTML only.
    ``?format=pdf`` renders HTML files to PDF server-side; other values serve
    normally. ``scale``, ``page_numbers``, and ``branding`` apply only with
    ``format=pdf``.
    """
    if format == "pdf":
        return await render_workspace_file_pdf(
            workspace_id, path, scale=scale, page_numbers=page_numbers, branding=branding
        )
    return await serve_workspace_file(
        workspace_id, path, inject_theme=(inject == "theme")
    )

