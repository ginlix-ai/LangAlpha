"""Unit tests for the unauthenticated workspace file-serving endpoint.

Calls ``serve_workspace_file`` / ``serve_workspace_file_endpoint`` directly
(same style as ``test_workspace_files_routing.py``) so no TestClient is
needed. The serving core resolves a workspace by UUID, reads bytes from the
live sandbox or the DB fallback, applies the sandboxed CSP, redacts vault
secrets from text bodies, and optionally splices a theme-sync script into HTML.

Covered: MIME mapping, uniform-404 (unknown workspace / missing file /
traversal), DB fallback (text + binary), CSP header on every response,
redaction of text bodies, ``?inject=theme`` splicing for HTML only, and
byte-faithful plain GET.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.app.workspace_files.serve import (
    _guess_content_type,
    _has_traversal,
    serve_workspace_file,
    serve_workspace_file_endpoint,
)
from src.server.services import pdf_render

WS_ID = "ws-test-0001"
OWNER = "user-test-1"
_VAULT_PATCH = "src.server.app.workspace_files.serve.get_vault_secrets_for_redaction"
_DBWS_PATCH = "src.server.app.workspace_files.serve.db_get_workspace"
_FP_PATCH = "src.server.app.workspace_files.serve.FilePersistenceService"
_WD_PATCH = "src.server.app.workspace_files.serve._get_work_dir"
_SANDBOX_PATCH = "src.server.app.workspace_files.serve._acquire_sandbox"
_WSMGR_PATCH = "src.server.app.workspace_files.serve.WorkspaceManager"
_RENDER_PATCH = "src.server.services.pdf_render.render_workspace_pdf"
_PDF_INTERNAL_BASE = "http://127.0.0.1:8000"


def _assert_report_csp(csp: str) -> None:
    """Assert the served-report CSP keeps the sandbox AND caps egress.

    Checks shape, not the exact string, so directive ordering can change freely.
    """
    assert csp.startswith("sandbox allow-scripts;")
    assert "default-src 'none'" in csp
    assert "connect-src 'none'" in csp  # the load-bearing exfiltration block
    # Google Fonts stays allowed for the CJK web-font path.
    assert "https://fonts.googleapis.com" in csp
    assert "https://fonts.gstatic.com" in csp


def _warm_manager() -> MagicMock:
    """A WorkspaceManager whose cached session reports ready (live-read path)."""
    mgr = MagicMock()
    mgr.get_instance.return_value.has_ready_session.return_value = True
    return mgr


def _workspace(status: str) -> dict:
    return {
        "workspace_id": WS_ID,
        "user_id": OWNER,
        "status": status,
        "config": None,
        "sandbox_id": "sb-existing",
    }


def _db_text_record(text: str, mime: str | None = "text/html") -> dict:
    return {
        "file_name": "report.html",
        "content_text": text,
        "content_binary": None,
        "is_binary": False,
        "mime_type": mime,
    }


def _db_binary_record(data: bytes, mime: str = "image/png") -> dict:
    return {
        "file_name": "chart.png",
        "content_text": None,
        "content_binary": data,
        "is_binary": True,
        "mime_type": mime,
    }


def _running_sandbox(returns: bytes | None) -> MagicMock:
    sandbox = MagicMock()
    sandbox.validate_and_normalize_path.return_value = ("/home/workspace/results/x", None)
    sandbox.adownload_file_bytes = AsyncMock(return_value=returns)
    sandbox.virtualize_path.return_value = "/results/x"
    return sandbox


# --- MIME mapping ---------------------------------------------------------


def test_mime_mapping_common_web_types():
    assert _guess_content_type("results/report.html") == "text/html; charset=utf-8"
    assert _guess_content_type("app.js") == "text/javascript; charset=utf-8"
    assert _guess_content_type("style.css") == "text/css; charset=utf-8"
    assert _guess_content_type("data.json") == "application/json; charset=utf-8"
    assert _guess_content_type("icon.svg") == "image/svg+xml"
    assert _guess_content_type("chart.png") == "image/png"
    assert _guess_content_type("photo.jpg") == "image/jpeg"
    assert _guess_content_type("font.woff2") == "font/woff2"


def test_mime_mapping_unknown_extension_is_octet_stream():
    assert _guess_content_type("blob.zzz") == "application/octet-stream"


# --- Traversal rejection → uniform 404 ------------------------------------


def test_has_traversal_helper():
    assert _has_traversal("a/../b")
    assert _has_traversal("../secret")
    assert _has_traversal("results\\..\\secret")
    assert not _has_traversal("results/report.html")
    assert not _has_traversal("a..b/c.html")  # dotdot inside a segment is allowed


@pytest.mark.asyncio
async def test_traversal_returns_uniform_404():
    # Traversal is rejected before any workspace lookup happens.
    with patch(_DBWS_PATCH, new=AsyncMock()) as mock_ws:
        with pytest.raises(HTTPException) as exc:
            await serve_workspace_file(WS_ID, "results/../../etc/passwd", inject_theme=False)
    assert exc.value.status_code == 404
    assert exc.value.detail == "Not found"
    mock_ws.assert_not_called()


# --- System / hidden agent-infra dirs are never served --------------------


@pytest.mark.asyncio
@patch(_FP_PATCH)
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_DBWS_PATCH, new_callable=AsyncMock)
@pytest.mark.parametrize(
    "blocked_path",
    [
        ".agents/user/memory/memory.md",
        "tools/generated_wrapper.py",
        "mcp_servers/server.py",
        ".system/config.json",
        "_internal/secret.txt",
    ],
)
async def test_system_and_hidden_paths_return_404(
    mock_ws, _wd, mock_fp, blocked_path
):
    # The serve core must mirror the read/download/list gate so the
    # unauthenticated route never exposes agent-infrastructure dirs, even
    # when a record exists in the DB fallback.
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(return_value=_db_text_record("secret"))
    with pytest.raises(HTTPException) as exc:
        await serve_workspace_file(WS_ID, blocked_path, inject_theme=False)
    assert exc.value.status_code == 404
    # Blocked before the DB is ever consulted.
    mock_fp.get_file_content.assert_not_called()


# --- Unknown / flash workspace → uniform 404 ------------------------------


@pytest.mark.asyncio
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_unknown_workspace_returns_uniform_404(mock_ws, _wd):
    mock_ws.return_value = None
    with pytest.raises(HTTPException) as exc:
        await serve_workspace_file(WS_ID, "results/report.html", inject_theme=False)
    assert exc.value.status_code == 404
    assert exc.value.detail == "Not found"


@pytest.mark.asyncio
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_db_lookup_error_returns_uniform_404(mock_ws, _wd):
    mock_ws.side_effect = RuntimeError("db down")
    with pytest.raises(HTTPException) as exc:
        await serve_workspace_file(WS_ID, "results/report.html", inject_theme=False)
    assert exc.value.status_code == 404
    assert exc.value.detail == "Not found"


@pytest.mark.asyncio
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_flash_workspace_returns_uniform_404(mock_ws, _wd):
    mock_ws.return_value = _workspace("flash")
    with pytest.raises(HTTPException) as exc:
        await serve_workspace_file(WS_ID, "results/report.html", inject_theme=False)
    assert exc.value.status_code == 404


# --- DB fallback (stopped workspace) --------------------------------------


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_db_fallback_serves_text(mock_ws, mock_fp, _wd, _vault):
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(
        return_value=_db_text_record("<html><head></head><body>hi</body></html>")
    )
    resp = await serve_workspace_file(WS_ID, "results/report.html", inject_theme=False)
    assert resp.status_code == 200
    assert resp.media_type == "text/html; charset=utf-8"
    assert b"<body>hi</body>" in resp.body
    mock_fp.get_file_content.assert_awaited_once_with(WS_ID, "results/report.html")


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_db_fallback_serves_binary(mock_ws, mock_fp, _wd, _vault):
    png = b"\x89PNG\r\n\x1a\n\x00\x01\x02\x03binarydata"
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(return_value=_db_binary_record(png))
    resp = await serve_workspace_file(WS_ID, "results/chart.png", inject_theme=False)
    assert resp.status_code == 200
    assert resp.media_type == "image/png"
    assert resp.body == png  # exact bytes, no redaction on binary


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_db_fallback_missing_file_returns_404(mock_ws, mock_fp, _wd, _vault):
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(return_value=None)
    with pytest.raises(HTTPException) as exc:
        await serve_workspace_file(WS_ID, "results/missing.html", inject_theme=False)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_db_fallback_unknown_extension_uses_db_mime(mock_ws, mock_fp, _wd, _vault):
    record = {
        "file_name": "data.bin",
        "content_text": None,
        "content_binary": b"raw",
        "is_binary": True,
        "mime_type": "application/x-custom",
    }
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(return_value=record)
    resp = await serve_workspace_file(WS_ID, "results/data.zzz", inject_theme=False)
    assert resp.media_type == "application/x-custom"


# --- Running sandbox source -----------------------------------------------


@pytest.mark.asyncio
@patch(_WSMGR_PATCH, new=_warm_manager())
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_SANDBOX_PATCH, new_callable=AsyncMock)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_running_workspace_serves_live_bytes(mock_ws, mock_sb, _wd, _vault):
    mock_ws.return_value = _workspace("running")
    mock_sb.return_value = _running_sandbox(b"<html><body>live</body></html>")
    resp = await serve_workspace_file(WS_ID, "results/x.html", inject_theme=False)
    assert resp.status_code == 200
    assert b"live" in resp.body


@pytest.mark.asyncio
@patch(_WSMGR_PATCH, new=_warm_manager())
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_SANDBOX_PATCH, new_callable=AsyncMock)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_running_workspace_missing_file_returns_404(mock_ws, mock_sb, _wd, _vault):
    mock_ws.return_value = _workspace("running")
    mock_sb.return_value = _running_sandbox(None)
    with pytest.raises(HTTPException) as exc:
        await serve_workspace_file(WS_ID, "results/x.html", inject_theme=False)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
@patch(_WSMGR_PATCH)
@patch(_FP_PATCH)
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_SANDBOX_PATCH, new_callable=AsyncMock)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_running_db_row_but_cold_sandbox_uses_db_not_wake(
    mock_ws, mock_sb, _wd, _vault, mock_fp, mock_mgr
):
    # DB says 'running' but no warm session (Daytona auto-stopped). The serve
    # route must read from the DB and never call _acquire_sandbox (which would
    # trigger a paid Daytona start) — denial-of-wallet guard.
    mock_ws.return_value = _workspace("running")
    mock_mgr.get_instance.return_value.has_ready_session.return_value = False
    mock_fp.get_file_content = AsyncMock(return_value=_db_text_record("from-db"))
    resp = await serve_workspace_file(WS_ID, "results/x.html", inject_theme=False)
    assert resp.status_code == 200
    assert b"from-db" in resp.body
    mock_sb.assert_not_called()


@pytest.mark.asyncio
@patch(_WSMGR_PATCH, new=_warm_manager())
@patch(_FP_PATCH)
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_SANDBOX_PATCH, new_callable=AsyncMock)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_warm_session_dies_falls_back_to_db_not_503(
    mock_ws, mock_sb, _wd, _vault, mock_fp
):
    # TOCTOU: has_ready_session() reported warm, but the session died before
    # _acquire_sandbox(), which then raises HTTPException(503). The serve route
    # must absorb that and fall back to the DB record — never leak a 503, which
    # would break the uniform-404 contract and confirm the UUID is valid.
    mock_ws.return_value = _workspace("running")
    mock_sb.side_effect = HTTPException(status_code=503, detail="Sandbox not ready")
    mock_fp.get_file_content = AsyncMock(return_value=_db_text_record("from-db"))
    resp = await serve_workspace_file(WS_ID, "results/x.html", inject_theme=False)
    assert resp.status_code == 200
    assert b"from-db" in resp.body


# --- CSP + cache headers present on every response ------------------------


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_csp_header_present_on_html(mock_ws, mock_fp, _wd, _vault):
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(return_value=_db_text_record("<html></html>"))
    resp = await serve_workspace_file(WS_ID, "results/report.html", inject_theme=False)
    _assert_report_csp(resp.headers["Content-Security-Policy"])
    assert resp.headers["Cache-Control"] == "private, max-age=60"
    assert resp.headers["Content-Disposition"].startswith("inline")
    assert "attachment" not in resp.headers["Content-Disposition"]


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_csp_header_present_on_binary(mock_ws, mock_fp, _wd, _vault):
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(return_value=_db_binary_record(b"\x89PNGbytes"))
    resp = await serve_workspace_file(WS_ID, "results/chart.png", inject_theme=False)
    # CSP is on EVERY response, not just HTML.
    _assert_report_csp(resp.headers["Content-Security-Policy"])
    assert resp.headers["Cache-Control"] == "private, max-age=60"


# --- Vault-secret redaction for text content ------------------------------


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock)
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_redaction_applied_to_text(mock_ws, mock_fp, _wd, mock_vault):
    secret = "SUPERSECRETVALUE123"
    mock_vault.return_value = {"API_KEY": secret}
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(
        return_value=_db_text_record(f"<html><body>key={secret}</body></html>")
    )
    resp = await serve_workspace_file(WS_ID, "results/report.html", inject_theme=False)
    assert secret.encode() not in resp.body
    assert b"[REDACTED:API_KEY]" in resp.body


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock)
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_redaction_not_applied_to_binary(mock_ws, mock_fp, _wd, mock_vault):
    # A binary PNG that happens to contain the secret byte-sequence is served
    # verbatim — redaction only runs on text content types.
    secret = "SUPERSECRETVALUE123"
    mock_vault.return_value = {"API_KEY": secret}
    raw = b"\x89PNG" + secret.encode() + b"\x00\x01"
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(return_value=_db_binary_record(raw))
    resp = await serve_workspace_file(WS_ID, "results/chart.png", inject_theme=False)
    assert resp.body == raw


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock)
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_redaction_applied_to_misnamed_text(mock_ws, mock_fp, _wd, mock_vault):
    # A UTF-8 text body served under a binary extension (e.g. secret.png) is
    # still redacted — the extension-derived MIME must not gate redaction, or a
    # secret in a mis-named file would leak on this unauthenticated route.
    secret = "SUPERSECRETVALUE123"
    mock_vault.return_value = {"API_KEY": secret}
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(
        return_value=_db_text_record(f"leaked key={secret}")
    )
    resp = await serve_workspace_file(WS_ID, "results/secret.png", inject_theme=False)
    assert resp.media_type == "image/png"
    assert secret.encode() not in resp.body
    assert b"[REDACTED:API_KEY]" in resp.body


# --- ?inject=theme splices for HTML only ----------------------------------


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_inject_theme_splices_for_html(mock_ws, mock_fp, _wd, _vault):
    mock_ws.return_value = _workspace("stopped")
    html = "<html><head><title>x</title></head><body>hi</body></html>"
    mock_fp.get_file_content = AsyncMock(return_value=_db_text_record(html))
    resp = await serve_workspace_file(WS_ID, "results/report.html", inject_theme=True)
    body = resp.body.decode()
    assert "widget:themeUpdate" in body
    assert 'content="light dark"' in body
    # Script spliced right after <head>, before the original <title>.
    assert body.index("widget:themeUpdate") < body.index("<title>")


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_inject_theme_not_applied_to_non_html(mock_ws, mock_fp, _wd, _vault):
    mock_ws.return_value = _workspace("stopped")
    css = "body{color:red}"
    mock_fp.get_file_content = AsyncMock(
        return_value=_db_text_record(css, mime="text/css")
    )
    resp = await serve_workspace_file(WS_ID, "results/style.css", inject_theme=True)
    assert resp.body == css.encode()
    assert b"widget:themeUpdate" not in resp.body


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_plain_get_is_byte_faithful(mock_ws, mock_fp, _wd, _vault):
    mock_ws.return_value = _workspace("stopped")
    html = "<html><head></head><body>exact bytes</body></html>"
    mock_fp.get_file_content = AsyncMock(return_value=_db_text_record(html))
    resp = await serve_workspace_file(WS_ID, "results/report.html", inject_theme=False)
    # No inject param → original bytes, no theme script.
    assert resp.body == html.encode()
    assert b"widget:themeUpdate" not in resp.body


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_inject_theme_skips_non_utf8_html(mock_ws, mock_fp, _wd, _vault):
    mock_ws.return_value = _workspace("stopped")
    # GBK-encoded HTML (the body bytes \xb1\xa8\xb8\xe6 are invalid UTF-8).
    # Injection must decline rather than corrupt it via an errors="replace"
    # decode, so the document is served byte-faithful with no theme script.
    gbk_html = b"<html><head></head><body>\xb1\xa8\xb8\xe6</body></html>"
    mock_fp.get_file_content = AsyncMock(
        return_value=_db_binary_record(gbk_html, mime="text/html")
    )
    resp = await serve_workspace_file(WS_ID, "results/report.html", inject_theme=True)
    assert resp.body == gbk_html
    assert b"widget:themeUpdate" not in resp.body


# --- Endpoint wrapper: ?inject query param wiring -------------------------


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_endpoint_inject_theme_query_enables_splice(mock_ws, mock_fp, _wd, _vault):
    mock_ws.return_value = _workspace("stopped")
    html = "<html><head></head><body>x</body></html>"
    mock_fp.get_file_content = AsyncMock(return_value=_db_text_record(html))
    resp = await serve_workspace_file_endpoint(
        workspace_id=WS_ID, path="results/report.html", inject="theme"
    )
    assert b"widget:themeUpdate" in resp.body


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_endpoint_without_inject_is_byte_faithful(mock_ws, mock_fp, _wd, _vault):
    mock_ws.return_value = _workspace("stopped")
    html = "<html><head></head><body>x</body></html>"
    mock_fp.get_file_content = AsyncMock(return_value=_db_text_record(html))
    resp = await serve_workspace_file_endpoint(
        workspace_id=WS_ID, path="results/report.html", inject=None
    )
    assert resp.body == html.encode()


# --- ?format=pdf: render HTML to PDF --------------------------------------
#
# render_workspace_pdf is mocked everywhere — CI has no Chromium. The pre-
# validation path (resolve bytes + require HTML) reuses the DB-fallback fixtures.

_EXPECTED_INTERNAL_URL = f"{_PDF_INTERNAL_BASE}/api/v1/wsfiles/{WS_ID}/results/report.html"
_EXPECTED_SERVE_PREFIX = f"{_PDF_INTERNAL_BASE}/api/v1/wsfiles/{WS_ID}/"


@pytest.mark.asyncio
@patch(_RENDER_PATCH, new_callable=AsyncMock)
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_format_pdf_renders_html(mock_ws, mock_fp, _wd, _vault, mock_render):
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(
        return_value=_db_text_record("<html><body>report</body></html>")
    )
    mock_render.return_value = b"%PDF-1.7 fake pdf bytes"

    resp = await serve_workspace_file_endpoint(
        workspace_id=WS_ID,
        path="results/report.html",
        inject=None,
        format="pdf",
        scale=None,
        page_numbers=False,
        branding=True,
    )

    assert resp.status_code == 200
    assert resp.media_type == "application/pdf"
    assert resp.body == b"%PDF-1.7 fake pdf bytes"
    cd = resp.headers["Content-Disposition"]
    assert cd.startswith("attachment")
    assert 'filename="report.pdf"' in cd
    assert resp.headers["Cache-Control"] == "private, max-age=60"
    # No CSP on the PDF response.
    assert "Content-Security-Policy" not in resp.headers
    mock_render.assert_awaited_once_with(
        _EXPECTED_INTERNAL_URL,
        workspace_serve_prefix=_EXPECTED_SERVE_PREFIX,
        scale=None,
        page_numbers=False,
        branding=True,
    )


@pytest.mark.asyncio
@patch(_RENDER_PATCH, new_callable=AsyncMock)
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_format_pdf_scale_and_page_numbers_pass_through(
    mock_ws, mock_fp, _wd, _vault, mock_render
):
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(
        return_value=_db_text_record("<html><body>report</body></html>")
    )
    mock_render.return_value = b"%PDF-1.7 fake pdf bytes"

    resp = await serve_workspace_file_endpoint(
        workspace_id=WS_ID,
        path="results/report.html",
        inject=None,
        format="pdf",
        scale=0.8,
        page_numbers=True,
        branding=False,
    )

    assert resp.status_code == 200
    mock_render.assert_awaited_once_with(
        _EXPECTED_INTERNAL_URL,
        workspace_serve_prefix=_EXPECTED_SERVE_PREFIX,
        scale=0.8,
        page_numbers=True,
        branding=False,
    )


@pytest.mark.asyncio
@patch(_RENDER_PATCH, new_callable=AsyncMock)
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_format_pdf_on_non_html_returns_404_no_render(
    mock_ws, mock_fp, _wd, _vault, mock_render
):
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(
        return_value=_db_text_record("body{color:red}", mime="text/css")
    )
    with pytest.raises(HTTPException) as exc:
        await serve_workspace_file_endpoint(
            workspace_id=WS_ID, path="results/style.css", inject=None, format="pdf"
        )
    assert exc.value.status_code == 404
    assert exc.value.detail == "Not found"
    mock_render.assert_not_awaited()


@pytest.mark.asyncio
@patch(_RENDER_PATCH, new_callable=AsyncMock)
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_format_pdf_on_missing_file_returns_404_no_render(
    mock_ws, mock_fp, _wd, _vault, mock_render
):
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(return_value=None)
    with pytest.raises(HTTPException) as exc:
        await serve_workspace_file_endpoint(
            workspace_id=WS_ID, path="results/missing.html", inject=None, format="pdf"
        )
    assert exc.value.status_code == 404
    mock_render.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "render_exc, expected_status",
    [
        (pdf_render.PdfRenderUnavailable("no chromium"), 501),
        (pdf_render.PdfRenderTimeout("timed out"), 504),
        (pdf_render.PdfRenderError("boom"), 500),
    ],
)
@patch(_RENDER_PATCH, new_callable=AsyncMock)
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_format_pdf_render_errors_map_to_status(
    mock_ws, mock_fp, _wd, _vault, mock_render, render_exc, expected_status
):
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(
        return_value=_db_text_record("<html><body>x</body></html>")
    )
    mock_render.side_effect = render_exc
    with pytest.raises(HTTPException) as exc:
        await serve_workspace_file_endpoint(
            workspace_id=WS_ID, path="results/report.html", inject=None, format="pdf"
        )
    assert exc.value.status_code == expected_status


@pytest.mark.asyncio
@patch(_RENDER_PATCH, new_callable=AsyncMock)
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_format_pdf_traversal_returns_404_no_render(
    mock_ws, mock_fp, _wd, _vault, mock_render
):
    with pytest.raises(HTTPException) as exc:
        await serve_workspace_file_endpoint(
            workspace_id=WS_ID,
            path="results/../../etc/passwd",
            inject=None,
            format="pdf",
        )
    assert exc.value.status_code == 404
    mock_render.assert_not_awaited()


@pytest.mark.asyncio
@patch(_RENDER_PATCH, new_callable=AsyncMock)
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_unknown_format_serves_normally(mock_ws, mock_fp, _wd, _vault, mock_render):
    mock_ws.return_value = _workspace("stopped")
    html = "<html><head></head><body>plain</body></html>"
    mock_fp.get_file_content = AsyncMock(return_value=_db_text_record(html))
    # An unrecognized format value falls through to a normal inline serve.
    resp = await serve_workspace_file_endpoint(
        workspace_id=WS_ID, path="results/report.html", inject=None, format="docx"
    )
    assert resp.status_code == 200
    assert resp.media_type == "text/html; charset=utf-8"
    assert resp.body == html.encode()
    mock_render.assert_not_awaited()


# --- ?format=pdf: internal-URL encoding (P2) ------------------------------
#
# normalized_path is interpolated into the internal wsfiles URL handed to
# headless Chromium. URL metacharacters and non-ASCII (CJK) must be UTF-8
# percent-encoded so Chromium hits the right file; slashes stay literal so the
# path structure survives. The serve endpoint decodes the segments back.


@pytest.mark.asyncio
@patch(_RENDER_PATCH, new_callable=AsyncMock)
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_format_pdf_encodes_url_metacharacters(
    mock_ws, mock_fp, _wd, _vault, mock_render
):
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(
        return_value=_db_text_record("<html><body>x</body></html>")
    )
    mock_render.return_value = b"%PDF-1.7 fake"

    # A space and a '#' in the filename would otherwise truncate/misroute.
    await serve_workspace_file_endpoint(
        workspace_id=WS_ID,
        path="results/Q4 report#draft.html",
        inject=None,
        format="pdf",
    )

    internal_url = mock_render.await_args.args[0]
    assert internal_url == (
        f"{_PDF_INTERNAL_BASE}/api/v1/wsfiles/{WS_ID}/"
        "results/Q4%20report%23draft.html"
    )
    # Slashes preserved, no raw space or '#' leaked into the URL.
    assert " " not in internal_url and "#" not in internal_url


@pytest.mark.asyncio
@patch(_RENDER_PATCH, new_callable=AsyncMock)
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_format_pdf_encodes_cjk_filename(
    mock_ws, mock_fp, _wd, _vault, mock_render
):
    mock_ws.return_value = _workspace("stopped")
    mock_fp.get_file_content = AsyncMock(
        return_value=_db_text_record("<html><body>报告</body></html>")
    )
    mock_render.return_value = b"%PDF-1.7 fake"

    # A CJK-named report (neutral placeholder name).
    await serve_workspace_file_endpoint(
        workspace_id=WS_ID,
        path="results/报告.html",
        inject=None,
        format="pdf",
    )

    internal_url = mock_render.await_args.args[0]
    # UTF-8 percent-encoding of 报告; the leading dir + '/' stay literal.
    assert internal_url == (
        f"{_PDF_INTERNAL_BASE}/api/v1/wsfiles/{WS_ID}/"
        "results/%E6%8A%A5%E5%91%8A.html"
    )
    serve_prefix = mock_render.await_args.kwargs["workspace_serve_prefix"]
    assert serve_prefix == _EXPECTED_SERVE_PREFIX


@pytest.mark.asyncio
@patch(_VAULT_PATCH, new_callable=AsyncMock, return_value={})
@patch(_WD_PATCH, return_value="/home/workspace")
@patch(_FP_PATCH)
@patch(_DBWS_PATCH, new_callable=AsyncMock)
async def test_serves_cjk_named_html_file(mock_ws, mock_fp, _wd, _vault):
    # The serve path receives the already-decoded unicode path (FastAPI decodes
    # the {path:path} segment) and must resolve + serve it as UTF-8.
    mock_ws.return_value = _workspace("stopped")
    html = "<html><head></head><body>季度报告</body></html>"
    mock_fp.get_file_content = AsyncMock(return_value=_db_text_record(html))
    resp = await serve_workspace_file(WS_ID, "results/报告.html", inject_theme=False)
    assert resp.status_code == 200
    assert resp.media_type == "text/html; charset=utf-8"
    assert "季度报告".encode() in resp.body
