"""Path-style workspace file serving (`/api/v1/wsfiles/g/<grant>/...`)."""

from __future__ import annotations

import asyncio
import logging
from typing import Annotated, Any
from urllib.parse import quote

from fastapi import APIRouter, Header, HTTPException, Query

from src.config.env import PDF_RENDER_INTERNAL_BASE
from src.server.preview_proxy.page_bridge import (
    Inject,
    bridge_for,
    parse_inject,
    with_bridge,
)
from src.server.services.file_grants import (
    FileGrantError,
    grant_prefix,
    mint_file_grant,
    verify_file_grant,
)
from src.server.utils.error_sanitization import single_line
from src.server.utils.html_cdns import html_document_csp
from src.server.utils.http_headers import content_disposition
from src.server.utils.served_origins import served_sources
from fastapi.responses import Response

from src.server.database.home_workspace import is_unbound
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.services.workspace_layout import WorkspaceLayoutUnavailable
from src.server.utils.secret_redactor import (
    get_redactor,
    get_vault_secrets_for_redaction,
)
from src.utils.mime import resolve_content_type

from src.server.services.workspace_file_containment import (
    contained_relative_path,
    FileTooLargeToServe,
)
from src.server.services.warm_files import (
    VisibilityGate,
    db_fallback_bytes,
    warm_sandbox,
    warm_sandbox_bytes,
)
from src.server.services.served_paths import (
    is_serve_blocked_path,
    is_text_content_type,
    is_utf8,
)
from src.server.services.workspace_files import previous_dir_names_of, record_fs_bytes

from ._shared import (
    folder_unmoved,
    work_dir_for,
)


def _default_visible(client_path: str) -> bool:
    return not is_serve_blocked_path(client_path)


logger = logging.getLogger(__name__)


def _served_work_dir(workspace: dict[str, Any], workspace_id: str) -> str:
    """The serve root, or this route's 404 for a workspace that names no folder.

    The URL is the only credential here, so every failed check answers the
    same way and none of them says which one failed. A row bound to a computer
    with no folder name is a workspace whose files cannot be placed, and the
    computer root is not a fallback: it holds the siblings.
    """
    try:
        return work_dir_for(workspace)
    except WorkspaceLayoutUnavailable as e:
        logger.warning(
            f"Refusing file access to workspace {workspace_id}: {single_line(str(e))}"
        )
        raise HTTPException(status_code=404, detail="Not found") from None


# ---------------------------------------------------------------------------
# Path-style file serving (`/api/v1/wsfiles/g/<grant>/...`)
# ---------------------------------------------------------------------------
#
# Gives `.html` deliverables true served semantics: a document served at
# `/wsfiles/g/<grant>/work/task/report.html` can reference `charts/foo.png`
# and the browser resolves it relatively under the same prefix. The grant is
# the credential: signed for the owner, expiring, and minted only by an
# authenticated route (``services/file_grants``). Uniform 404 for a bad grant,
# a missing file and an unauthorized path alike, and never wake a stopped
# sandbox (denial-of-wallet protection).
#
# This is the owner's own viewer mechanism, NOT a sharing primitive: a grant
# opens the whole workspace. User-facing sharing goes through share links and
# permission-scoped thread-share tokens.

wsfiles_router = APIRouter(prefix="/api/v1", tags=["Workspace File Serving"])

# Short private cache: HTML reports and their assets are effectively immutable
# for a turn, so up to 60s of staleness on reload (until the next agent update
# is picked up) is an acceptable trade for far fewer sandbox/DB reads. The
# grant is a bearer credential, so we never allow shared/public caches to
# retain the bytes.
_WSFILES_CACHE_CONTROL = "private, max-age=60"

# The renderer fetches the document and its subresources with no credential
# of its own, so the prefix it is handed carries one that outlives the render
# by a few minutes and nothing else.
_PDF_GRANT_TTL_SECONDS = 5 * 60

# Content-Security-Policy for served reports (``utils/html_cdns``). Three jobs:
#   1. The `sandbox` directive forces an opaque origin even though the iframe
#      loads via `src=`, so agent/prompt-injected HTML can never reach app
#      cookies/localStorage. (The header intersects with the iframe's sandbox
#      attribute; both carry the popup tokens.)
#   2. connect-src names the route prefix the report was served under, and
#      nothing else: the owner's grant prefix, which opens the workspace, or a
#      share's prefix, which opens what the share does. The route still refuses
#      hidden and serve-blocked paths. So a report can fetch any file it could
#      link to, while no fetch, beacon or socket reaches another API route,
#      another grant or another host. A path in a source needs an absolute
#      origin, which comes from configuration (`utils/served_origins`).
#   3. The other source directives cap loads to the html-report skill's CDN
#      allowlist.
# connect-src stops background requests only. A frame navigating itself away
# is stopped by the embedding app's frame-src (web/index.html). What remains is
# a click on a link in a tampered report: the new tab's URL can carry whatever
# the report has read, which for the owner's view is any file in the workspace.
def _wsfiles_csp(reach: str | None, host: str | None) -> str:
    """The served-file CSP whose connect-src is ``reach`` on each served origin."""
    return html_document_csp(
        served_sources(reach, host) if reach else [], published=False
    )


def _guess_content_type(path: str) -> str:
    """Resolve a Content-Type for a served file via the canonical pinned map."""
    return resolve_content_type(path)


def _is_html_content_type(content_type: str) -> bool:
    return content_type.split(";", 1)[0].strip().lower() in ("text/html", "text/htm")


def _has_traversal(path: str) -> bool:
    """Reject `..` segments before they reach path resolution."""
    return ".." in (path or "").replace("\\", "/").split("/")


async def _resolve_serve_bytes(
    workspace: dict[str, Any],
    workspace_id: str,
    normalized_path: str,
    *,
    work_dir: str,
    visible: VisibilityGate = _default_visible,
) -> tuple[bytes, str] | None:
    """Resolve raw bytes + content type for a file, sandbox-first with DB fallback.

    Returns ``(content, content_type)`` or ``None`` when the file is missing.
    This is the one ordering every route that serves a workspace file uses:
    live bytes from an already-warm, binding-matched session, and the persisted
    record otherwise. Two routes answering the same token with different bytes
    is the defect the single rule removes.
    """
    extension_mime = _guess_content_type(normalized_path)

    sandbox = warm_sandbox(workspace, workspace_id)
    if sandbox is None:
        return await db_fallback_bytes(
            workspace, workspace_id, normalized_path, extension_mime
        )
    try:
        resolved = await warm_sandbox_bytes(
            sandbox, normalized_path, work_dir=work_dir, visible=visible
        )
    except FileTooLargeToServe:
        return await db_fallback_bytes(
            workspace, workspace_id, normalized_path, extension_mime
        )
    except RuntimeError as e:
        # Deliberate residual: an unreachable sandbox is indistinguishable from a
        # missing file on this route. The URL is the only credential, so
        # distinguishing them (503 vs 404) would confirm that a guessed
        # credential resolved to a real workspace.
        # Fall back to the persisted copy first so the common case still serves.
        # Warning, not debug: the response deliberately hides the cause, which
        # makes this line the only place it survives.
        logger.warning(
            f"Sandbox read failed for workspace {workspace_id}; "
            f"serving the persisted copy instead: {single_line(str(e))}"
        )
        return await db_fallback_bytes(
            workspace, workspace_id, normalized_path, extension_mime
        )
    if resolved is None:
        return None
    # Read without a folder hold, so the folder ``work_dir`` was built on is
    # checked after the fact: a sibling may have landed on its name.
    if not await folder_unmoved(workspace_id, workspace.get("dir_name")):
        return None
    return resolved[1], extension_mime


async def serve_workspace_file(
    workspace_id: str,
    path: str,
    *,
    inject: Inject | None = None,
    workspace: dict[str, Any] | None = None,
    visible: VisibilityGate | None = None,
    reach: str | None = None,
    host: str | None = None,
) -> Response:
    """Serve one workspace file inline with a sandboxed CSP, bridged if asked.

    Resolves the file (running sandbox first, DB fallback for stopped
    workspaces), picks the Content-Type, redacts vault secrets from text
    bodies, and emits the sandboxed, egress-capped CSP (``_wsfiles_csp``) on
    every response. ``reach`` is the route prefix the file was served under,
    which its document may fetch from; without one it fetches nothing.
    ``host`` is the request's Host, which only picks among the configured
    origins (``utils/served_origins``). With ``inject`` an HTML body carries
    the page bridge a viewer frames it with (``preview_proxy/page_bridge``);
    otherwise the bytes are served faithfully. Missing, unknown, traversal and
    escaping inputs all raise a uniform 404 so the endpoint never reveals which
    check failed, and a 404 rather than a 403 so a denial never confirms that a
    path exists.

    ``workspace`` may be passed pre-resolved (e.g. by a share route) to reuse
    this core with a different credential resolver; otherwise the workspace is
    looked up by the id the caller's credential named. ``visible`` is that
    route's own reach: a share token scoped to one subtree or a share link
    frozen to a file list narrows it, and the gate runs again on whatever path
    the sandbox read actually resolved to.
    """
    visible = visible or _default_visible
    if _has_traversal(path):
        raise HTTPException(status_code=404, detail="Not found")

    if workspace is None:
        try:
            workspace = await db_get_workspace(workspace_id)
        except Exception:
            raise HTTPException(status_code=404, detail="Not found") from None
    if not workspace or is_unbound(workspace):
        raise HTTPException(status_code=404, detail="Not found")

    work_dir = _served_work_dir(workspace, workspace_id)
    normalized_path = contained_relative_path(
        path, work_dir, previous_dir_names_of(workspace)
    )
    if normalized_path is None or not visible(normalized_path):
        raise HTTPException(status_code=404, detail="Not found")

    resolved = await _resolve_serve_bytes(
        workspace, workspace_id, normalized_path, work_dir=work_dir, visible=visible
    )
    if resolved is None:
        raise HTTPException(status_code=404, detail="Not found")
    content, content_type = resolved

    # Redact vault secrets from any UTF-8-decodable body, not just declared
    # text MIME types — otherwise a secret written to a mis-named file (e.g.
    # secret.png) would bypass redaction on this unauthenticated, shareable
    # route. Genuine binary fails to decode and is served verbatim, so we also
    # skip the per-asset vault fetch for it.
    if is_text_content_type(content_type) or is_utf8(content):
        vault_secrets = await get_vault_secrets_for_redaction(workspace["user_id"])
        content = get_redactor().redact_bytes(content, vault_secrets=vault_secrets)

    if inject is not None and _is_html_content_type(content_type) and is_utf8(content):
        # Only a UTF-8 body is bridged. The bridge is ASCII, but a document in
        # another encoding (GBK, Shift-JIS) is served byte-faithful, as the
        # redaction above preserved it, rather than risk a splice the browser
        # reads as something else.
        content = await asyncio.to_thread(with_bridge, content, bridge_for(inject))

    headers = {
        "Content-Security-Policy": _wsfiles_csp(reach, host),
        # The frame's origin is opaque, so its fetches and module scripts reach
        # these files cross-origin, as Origin: null. The URL is the whole
        # credential, so any origin may read, and never with credentials
        # (``setup._URL_CREDENTIAL_ROUTES`` keeps the app's CORS off).
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": _WSFILES_CACHE_CONTROL,
        "Content-Disposition": content_disposition(
            normalized_path.rsplit("/", 1)[-1] or "file", disposition="inline"
        ),
        "X-Content-Type-Options": "nosniff",
    }
    record_fs_bytes("serve", len(content))
    return Response(content=content, media_type=content_type, headers=headers)


async def render_workspace_file_pdf(
    workspace_id: str,
    path: str,
    *,
    workspace: dict[str, Any] | None = None,
    scale: float | None = None,
    page_numbers: bool = False,
    branding: bool = True,
    visible: VisibilityGate | None = None,
    serve_base: str | None = None,
) -> Response:
    """Render a workspace HTML file to PDF via headless Chromium.

    Pre-validates the file exists and is HTML (same uniform 404 posture as the
    inline serve, so chromium never spins on garbage), then renders the byte-
    faithful loopback wsfiles URL (no bridge) under an SSRF-gated
    browser. Renderer failures map to 501/504/500 — intentionally NOT 404,
    since the file exists and only the converter failed.

    ``serve_base`` is the route prefix the document and every subresource it
    pulls are fetched under, and it is the whole authorization story for those
    subresources: the browser fetches them with no token of its own, so
    whatever the prefix admits is what the PDF can contain. The default is a
    grant that outlives the render by minutes; a caller whose reach is
    narrower than the workspace passes the prefix of a route that re-checks
    each request rather than a deeper string to match against.
    """
    visible = visible or _default_visible
    if _has_traversal(path):
        raise HTTPException(status_code=404, detail="Not found")

    if workspace is None:
        try:
            workspace = await db_get_workspace(workspace_id)
        except Exception:
            raise HTTPException(status_code=404, detail="Not found") from None
    if not workspace or is_unbound(workspace):
        raise HTTPException(status_code=404, detail="Not found")

    work_dir = _served_work_dir(workspace, workspace_id)
    normalized_path = contained_relative_path(
        path, work_dir, previous_dir_names_of(workspace)
    )
    if normalized_path is None or not visible(normalized_path):
        raise HTTPException(status_code=404, detail="Not found")

    # Cheap pre-validation: resolve bytes + content type and require HTML.
    resolved = await _resolve_serve_bytes(
        workspace, workspace_id, normalized_path, work_dir=work_dir, visible=visible
    )
    if resolved is None:
        raise HTTPException(status_code=404, detail="Not found")
    _content, content_type = resolved
    if not _is_html_content_type(content_type):
        raise HTTPException(status_code=404, detail="Not found")

    from src.server.services import pdf_render

    base = PDF_RENDER_INTERNAL_BASE.rstrip("/")
    if serve_base is None:
        grant = await mint_file_grant(workspace_id, ttl=_PDF_GRANT_TTL_SECONDS)
        serve_base = grant_prefix(grant)
    if not serve_base.startswith(base):
        serve_base = f"{base}/{serve_base.lstrip('/')}"
    # Percent-encode the path (UTF-8) so metacharacters (#, ?, space) and
    # non-ASCII (CJK) survive into headless Chromium; keep `/` so the path
    # structure stays intact. The serving endpoint decodes it back to unicode.
    internal_url = f"{serve_base}{quote(normalized_path, safe='/')}"
    try:
        pdf_bytes = await pdf_render.render_workspace_pdf(
            internal_url,
            workspace_serve_prefix=serve_base,
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
    record_fs_bytes("pdf", len(pdf_bytes))
    return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)


@wsfiles_router.get("/wsfiles/g/{grant}/{path:path}")
async def serve_workspace_file_endpoint(
    grant: str,
    path: str,
    inject: str | None = Query(
        None,
        description=(
            "Set to 'view' to splice the page bridge into HTML (theme, scroll "
            "and links), or 'comments' for the bridge with comment mode (the "
            "owner's live view). 'theme' is accepted as 'view'."
        ),
    ),
    format: str | None = Query(
        None, description="Set to 'pdf' to render HTML as a PDF."
    ),
    scale: float | None = Query(
        None, ge=0.5, le=2.0, description="PDF only: render scale (0.5–2.0)."
    ),
    page_numbers: bool = Query(
        False, description="PDF only: draw an 'N / total' footer in the page margin."
    ),
    branding: bool = Query(
        True, description="PDF only: stamp 'LangAlpha · <date>' in the footer."
    ),
    host: Annotated[str | None, Header()] = None,
) -> Response:
    """Serve a workspace file by path with sandboxed CSP.

    The grant is the credential; uniform 404 for a bad or expired grant, an
    unknown workspace, a missing file, or traversal. ``?inject=view`` adds the
    page bridge to HTML only, ``?inject=comments`` the bridge with comment
    mode (``page_bridge.parse_inject``). ``?format=pdf`` renders HTML files to PDF
    server-side; other values serve normally. ``scale``, ``page_numbers``,
    and ``branding`` apply only with ``format=pdf``.
    """
    try:
        workspace_id = await verify_file_grant(grant)
    except FileGrantError as e:
        logger.info(f"Refusing wsfiles request: {single_line(str(e))}")
        raise HTTPException(status_code=404, detail="Not found") from None
    if format == "pdf":
        return await render_workspace_file_pdf(
            workspace_id,
            path,
            scale=scale,
            page_numbers=page_numbers,
            branding=branding,
        )
    return await serve_workspace_file(
        workspace_id,
        path,
        inject=parse_inject(inject),
        reach=grant_prefix(grant),
        host=host,
    )
