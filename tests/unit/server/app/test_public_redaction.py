"""Unit tests for secret redaction in public share endpoints.

Tests the read_shared_file and download_shared_file endpoints with
mocked DB and patched SecretRedactor.

Note: public.py lazily imports db_get_workspace, FilePersistenceService,
and WorkspaceManager inside each handler. We patch at source module level
for those. Top-level imports (_normalize_requested_path, get_redactor)
are patched in the public module namespace.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app
from src.server.utils.secret_redactor import SecretRedactor

pytestmark = pytest.mark.asyncio

# Test data
_SECRET_NAME = "FMP_API_KEY"
_SECRET_VALUE = "sk_test_fmp_1234567890abcdef"
_SHARE_TOKEN = "share_abc123"
_WORKSPACE_ID = "ws-test-001"
_THREAD_ID = "thread-001"

# Patch targets
_THREAD_BY_TOKEN = "src.server.app.public.get_thread_by_share_token"
_DB_GET_WS = "src.server.database.workspace.get_workspace"
_FILE_SVC = "src.server.services.persistence.file.FilePersistenceService.get_file_content"
_NORM_PATH = "src.server.app.public._normalize_requested_path"
_WORK_DIR = "src.server.app.public._get_work_dir"
_GET_REDACTOR = "src.server.app.public.get_redactor"


def _make_thread(**overrides):
    thread = {
        "conversation_thread_id": _THREAD_ID,
        "workspace_id": _WORKSPACE_ID,
        "share_permissions": {"allow_files": True, "allow_download": True},
        "title": "Test Thread",
        "msg_type": "ptc",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "workspace_name": "Test Workspace",
    }
    thread.update(overrides)
    return thread


def _make_workspace(**overrides):
    ws = {
        "id": _WORKSPACE_ID,
        "user_id": "test-user-123",
        "workspace_id": _WORKSPACE_ID,
        "status": "running",
        "sandbox_id": "sb-123",
    }
    ws.update(overrides)
    return ws


def _make_file_record(content_text, **overrides):
    rec = {
        "content_text": content_text,
        "is_binary": False,
        "mime_type": "text/plain",
        "file_name": "test.txt",
    }
    rec.update(overrides)
    return rec


@pytest.fixture
def mock_redactor():
    r = SecretRedactor.__new__(SecretRedactor)
    r._secrets = [(_SECRET_NAME, _SECRET_VALUE)]
    return r


@pytest_asyncio.fixture
async def public_client():
    """httpx client wired to public router."""
    from src.server.app.public import router

    app = create_test_app(router)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# TestReadSharedFileRedaction
# ---------------------------------------------------------------------------


class TestReadSharedFileRedaction:
    """Verify read_shared_file redacts secrets from DB file records."""

    async def test_read_redacts_secret_from_db(self, public_client, mock_redactor):
        content = f"API_KEY={_SECRET_VALUE}\nother=safe"

        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_DB_GET_WS, AsyncMock(return_value=_make_workspace())),
            patch(_WORK_DIR, return_value="/home/workspace"),
            patch(_NORM_PATH, return_value="data/test.txt"),
            patch(_FILE_SVC, AsyncMock(return_value=_make_file_record(content))),
            patch(_GET_REDACTOR, return_value=mock_redactor),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/read",
                params={"path": "data/test.txt"},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert _SECRET_VALUE not in body["content"]
        assert f"[REDACTED:{_SECRET_NAME}]" in body["content"]
        assert "other=safe" in body["content"]

    async def test_read_no_redaction_when_clean(self, public_client):
        """Content without secrets passes through unchanged."""
        content = "clean data only"
        empty_redactor = SecretRedactor.__new__(SecretRedactor)
        empty_redactor._secrets = []

        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_DB_GET_WS, AsyncMock(return_value=_make_workspace())),
            patch(_WORK_DIR, return_value="/home/workspace"),
            patch(_NORM_PATH, return_value="data/clean.txt"),
            patch(_FILE_SVC, AsyncMock(return_value=_make_file_record(content))),
            patch(_GET_REDACTOR, return_value=empty_redactor),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/read",
                params={"path": "data/clean.txt"},
            )

        assert resp.status_code == 200
        assert resp.json()["content"] == content


# ---------------------------------------------------------------------------
# TestDownloadSharedFileRedaction
# ---------------------------------------------------------------------------


class TestDownloadSharedFileRedaction:
    """Verify download_shared_file redacts secrets from text files."""

    async def test_download_redacts_secret_from_text(self, public_client, mock_redactor):
        text = f"key={_SECRET_VALUE}"
        file_record = _make_file_record(
            content_text=text,
            mime_type="text/plain",
            file_name="config.txt",
        )

        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_DB_GET_WS, AsyncMock(return_value=_make_workspace())),
            patch(_WORK_DIR, return_value="/home/workspace"),
            patch(_NORM_PATH, return_value="config.txt"),
            patch(_FILE_SVC, AsyncMock(return_value=file_record)),
            patch(_GET_REDACTOR, return_value=mock_redactor),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/download",
                params={"path": "config.txt"},
            )

        assert resp.status_code == 200
        body = resp.content.decode("utf-8")
        assert _SECRET_VALUE not in body
        assert f"[REDACTED:{_SECRET_NAME}]" in body

    async def test_download_skips_redaction_for_binary(self, public_client, mock_redactor):
        """Binary files are not redacted even if they contain secret bytes."""
        binary_content = b"\x89PNG" + _SECRET_VALUE.encode()
        file_record = {
            "content_text": None,
            "content_binary": binary_content,
            "is_binary": True,
            "mime_type": "image/png",
            "file_name": "chart.png",
        }

        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_DB_GET_WS, AsyncMock(return_value=_make_workspace())),
            patch(_WORK_DIR, return_value="/home/workspace"),
            patch(_NORM_PATH, return_value="chart.png"),
            patch(_FILE_SVC, AsyncMock(return_value=file_record)),
            patch(_GET_REDACTOR, return_value=mock_redactor),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/download",
                params={"path": "chart.png"},
            )

        assert resp.status_code == 200
        # Binary should NOT be redacted
        assert _SECRET_VALUE.encode() in resp.content

    async def test_download_redacts_secret_from_json(self, public_client, mock_redactor):
        """Non-text MIME that is still UTF-8 text (application/json) is redacted —
        a vault secret must not leak just because the file isn't labeled text/*."""
        text = f'{{"api_key": "{_SECRET_VALUE}"}}'
        file_record = _make_file_record(
            content_text=text,
            mime_type="application/json",
            file_name="config.json",
        )

        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_DB_GET_WS, AsyncMock(return_value=_make_workspace())),
            patch(_WORK_DIR, return_value="/home/workspace"),
            patch(_NORM_PATH, return_value="config.json"),
            patch(_FILE_SVC, AsyncMock(return_value=file_record)),
            patch(_GET_REDACTOR, return_value=mock_redactor),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/download",
                params={"path": "config.json"},
            )

        assert resp.status_code == 200
        body = resp.content.decode("utf-8")
        assert _SECRET_VALUE not in body
        assert f"[REDACTED:{_SECRET_NAME}]" in body


# ---------------------------------------------------------------------------
# TestPublicTraversalGuard — `..` rejected before the sandbox path validator
# ---------------------------------------------------------------------------


class TestPublicTraversalGuard:
    """A `..` path 404s before reaching the DB/sandbox resolver, so the
    unresolved-`..` traversal in the sandbox path validator stays unreachable."""

    async def test_read_rejects_dotdot(self, public_client):
        file_svc = AsyncMock()
        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_FILE_SVC, file_svc),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/read",
                params={"path": "../../etc/passwd"},
            )
        assert resp.status_code == 404
        file_svc.assert_not_awaited()

    async def test_download_rejects_dotdot(self, public_client):
        file_svc = AsyncMock()
        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_FILE_SVC, file_svc),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/download",
                params={"path": "../../etc/passwd"},
            )
        assert resp.status_code == 404
        file_svc.assert_not_awaited()

    async def test_list_rejects_dotdot(self, public_client):
        with patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files",
                params={"path": "../sibling"},
            )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# TestServeSharedFile — GET /shared/{token}/files/serve/{path}
# ---------------------------------------------------------------------------

# serve_workspace_file resolves bytes / work dir / vault secrets from the
# workspace_files module, while the share endpoint resolves the thread and
# workspace from the public module. We patch each at its source.
_PUBLIC_DB_GET_WS = "src.server.app.public.db_get_workspace"
_WS_DB_GET_WS = "src.server.app.workspace_files.serve.db_get_workspace"
_WS_WORK_DIR = "src.server.app.workspace_files.serve._get_work_dir"
_WS_FP = "src.server.app.workspace_files.serve.FilePersistenceService"
_WS_VAULT = "src.server.app.workspace_files.serve.get_vault_secrets_for_redaction"
_RENDER = "src.server.services.pdf_render.render_workspace_pdf"
_PDF_BASE = "http://127.0.0.1:8000"


def _serve_text_record(text, mime="text/html"):
    return {
        "file_name": "report.html",
        "content_text": text,
        "content_binary": None,
        "is_binary": False,
        "mime_type": mime,
    }


def _serve_binary_record(data, mime="image/png"):
    return {
        "file_name": "chart.png",
        "content_text": None,
        "content_binary": data,
        "is_binary": True,
        "mime_type": mime,
    }


class TestServeSharedFile:
    """Path-style inline file serving over a share token."""

    async def test_unknown_token_returns_404(self, public_client):
        with patch(_THREAD_BY_TOKEN, AsyncMock(return_value=None)):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/serve/results/report.html"
            )
        assert resp.status_code == 404

    async def test_files_not_permitted_returns_403(self, public_client):
        thread = _make_thread(share_permissions={"allow_files": False})
        with patch(_THREAD_BY_TOKEN, AsyncMock(return_value=thread)):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/serve/results/report.html"
            )
        # Mirrors the existing files/read permission behavior (403 not granted).
        assert resp.status_code == 403

    async def test_serves_html_with_csp_and_mime(self, public_client):
        html = "<html><head></head><body>shared report</body></html>"
        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_PUBLIC_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="stopped"))),
            patch(_WS_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="stopped"))),
            patch(_WS_WORK_DIR, return_value="/home/workspace"),
            patch(_WS_VAULT, AsyncMock(return_value={})),
            patch(f"{_WS_FP}.get_file_content", AsyncMock(return_value=_serve_text_record(html))),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/serve/results/report.html"
            )
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "text/html; charset=utf-8"
        # Keep the sandbox AND cap egress (connect-src 'none'); assert shape, not
        # the exact string, so directive ordering can change freely.
        csp = resp.headers["content-security-policy"]
        assert csp.startswith("sandbox allow-scripts;")
        assert "default-src 'none'" in csp
        assert "connect-src 'none'" in csp
        assert "https://fonts.googleapis.com" in csp  # CJK web-font path
        assert "https://fonts.gstatic.com" in csp
        assert b"shared report" in resp.content
        # The workspace UUID must never leak into the response headers.
        for value in resp.headers.values():
            assert _WORKSPACE_ID not in value

    async def test_relative_subresource_resolves_under_token_prefix(self, public_client):
        # A relative `charts/x.png` reference from results/report.html resolves to
        # .../serve/results/charts/x.png; the endpoint serves it like any other path.
        png = b"\x89PNG\r\n\x1a\nchart-bytes"
        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_PUBLIC_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="stopped"))),
            patch(_WS_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="stopped"))),
            patch(_WS_WORK_DIR, return_value="/home/workspace"),
            patch(_WS_VAULT, AsyncMock(return_value={})),
            patch(
                f"{_WS_FP}.get_file_content",
                AsyncMock(return_value=_serve_binary_record(png)),
            ) as mock_content,
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/serve/results/charts/x.png"
            )
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "image/png"
        assert resp.content == png
        mock_content.assert_awaited_once_with(_WORKSPACE_ID, "results/charts/x.png")

    async def test_inject_theme_passthrough(self, public_client):
        html = "<html><head><title>r</title></head><body>x</body></html>"
        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_PUBLIC_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="stopped"))),
            patch(_WS_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="stopped"))),
            patch(_WS_WORK_DIR, return_value="/home/workspace"),
            patch(_WS_VAULT, AsyncMock(return_value={})),
            patch(f"{_WS_FP}.get_file_content", AsyncMock(return_value=_serve_text_record(html))),
        ):
            with_theme = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/serve/results/report.html",
                params={"inject": "theme"},
            )
            plain = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/serve/results/report.html"
            )
        assert b"widget:themeUpdate" in with_theme.content
        # No inject param → byte-faithful, no theme script.
        assert b"widget:themeUpdate" not in plain.content
        assert plain.content == html.encode()

    async def test_revoked_share_returns_404(self, public_client):
        # Toggling sharing off deletes the share-token row → lookup returns None.
        with patch(_THREAD_BY_TOKEN, AsyncMock(return_value=None)):
            resp = await public_client.get(
                "/api/v1/public/shared/revoked-token/files/serve/results/report.html"
            )
        assert resp.status_code == 404


class TestServeSharedFileErrorPage:
    """A revoked/forbidden shared link renders a branded HTML page for browsers,
    but keeps the raw JSON error for programmatic (non-HTML) clients."""

    _SERVE = "/api/v1/public/shared/revoked-token/files/serve/results/report.html"

    async def test_revoked_renders_html_page_for_browser(self, public_client):
        with patch(_THREAD_BY_TOKEN, AsyncMock(return_value=None)):
            resp = await public_client.get(self._SERVE, headers={"Accept": "text/html"})
        assert resp.status_code == 404
        assert resp.headers["content-type"].startswith("text/html")
        assert resp.headers["cache-control"] == "no-store"
        # A real page, not the raw FastAPI JSON detail.
        assert b"This shared report" in resp.content
        assert b'href="/"' in resp.content
        assert b'"detail"' not in resp.content

    async def test_revoked_keeps_json_for_api_client(self, public_client):
        # Default httpx Accept is */* (no text/html) → JSON error preserved.
        with patch(_THREAD_BY_TOKEN, AsyncMock(return_value=None)):
            resp = await public_client.get(self._SERVE)
        assert resp.status_code == 404
        assert resp.headers["content-type"].startswith("application/json")
        assert resp.json()["detail"] == "Shared thread not found"

    async def test_forbidden_renders_permission_page_for_browser(self, public_client):
        thread = _make_thread(share_permissions={"allow_files": False})
        with patch(_THREAD_BY_TOKEN, AsyncMock(return_value=thread)):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/serve/results/report.html",
                headers={"Accept": "text/html"},
            )
        assert resp.status_code == 403
        assert resp.headers["content-type"].startswith("text/html")
        assert b"file access" in resp.content

    async def test_error_page_localizes_to_chinese(self, public_client):
        with patch(_THREAD_BY_TOKEN, AsyncMock(return_value=None)):
            resp = await public_client.get(
                self._SERVE,
                headers={"Accept": "text/html", "Accept-Language": "zh-CN,zh;q=0.9"},
            )
        assert resp.status_code == 404
        assert "此分享报告不可用".encode() in resp.content


class TestServeSharedFilePdf:
    """?format=pdf over a share token — renderer is mocked (no Chromium in CI)."""

    _INTERNAL_URL = f"{_PDF_BASE}/api/v1/wsfiles/{_WORKSPACE_ID}/results/report.html"
    _SERVE_PREFIX = f"{_PDF_BASE}/api/v1/wsfiles/{_WORKSPACE_ID}/"

    async def test_format_pdf_renders_html(self, public_client):
        from src.server.services import pdf_render

        html = "<html><body>shared report</body></html>"
        render = AsyncMock(return_value=b"%PDF-1.7 shared")
        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_PUBLIC_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="stopped"))),
            patch(_WS_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="stopped"))),
            patch(_WS_WORK_DIR, return_value="/home/workspace"),
            patch(_WS_VAULT, AsyncMock(return_value={})),
            patch(f"{_WS_FP}.get_file_content", AsyncMock(return_value=_serve_text_record(html))),
            patch.object(pdf_render, "render_workspace_pdf", render),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/serve/results/report.html",
                params={"format": "pdf"},
            )
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/pdf"
        assert resp.content == b"%PDF-1.7 shared"
        cd = resp.headers["content-disposition"]
        assert cd.startswith("attachment")
        assert 'filename="report.pdf"' in cd
        # The workspace UUID must never leak into response headers.
        for value in resp.headers.values():
            assert _WORKSPACE_ID not in value
        render.assert_awaited_once_with(
            self._INTERNAL_URL,
            workspace_serve_prefix=self._SERVE_PREFIX,
            scale=None,
            page_numbers=False,
            branding=True,
        )

    async def test_format_pdf_not_permitted_returns_403(self, public_client):
        thread = _make_thread(share_permissions={"allow_files": False})
        render = AsyncMock()
        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=thread)),
            patch(_RENDER, render),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/serve/results/report.html",
                params={"format": "pdf"},
            )
        assert resp.status_code == 403
        render.assert_not_awaited()

    async def test_format_pdf_revoked_returns_404(self, public_client):
        render = AsyncMock()
        with patch(_THREAD_BY_TOKEN, AsyncMock(return_value=None)), patch(_RENDER, render):
            resp = await public_client.get(
                "/api/v1/public/shared/revoked-token/files/serve/results/report.html",
                params={"format": "pdf"},
            )
        assert resp.status_code == 404
        render.assert_not_awaited()

    async def test_format_pdf_on_non_html_returns_404(self, public_client):
        css = "body{color:red}"
        render = AsyncMock()
        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_PUBLIC_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="stopped"))),
            patch(_WS_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="stopped"))),
            patch(_WS_WORK_DIR, return_value="/home/workspace"),
            patch(_WS_VAULT, AsyncMock(return_value={})),
            patch(
                f"{_WS_FP}.get_file_content",
                AsyncMock(return_value=_serve_text_record(css, mime="text/css")),
            ),
            patch(_RENDER, render),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/serve/results/style.css",
                params={"format": "pdf"},
            )
        assert resp.status_code == 404
        render.assert_not_awaited()

    async def test_format_pdf_render_errors_map_to_status(self, public_client):
        from src.server.services import pdf_render

        html = "<html><body>x</body></html>"
        cases = [
            (pdf_render.PdfRenderUnavailable("no chromium"), 501),
            (pdf_render.PdfRenderTimeout("slow"), 504),
            (pdf_render.PdfRenderError("boom"), 500),
        ]
        for exc, status in cases:
            render = AsyncMock(side_effect=exc)
            with (
                patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
                patch(_PUBLIC_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="stopped"))),
                patch(_WS_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="stopped"))),
                patch(_WS_WORK_DIR, return_value="/home/workspace"),
                patch(_WS_VAULT, AsyncMock(return_value={})),
                patch(f"{_WS_FP}.get_file_content", AsyncMock(return_value=_serve_text_record(html))),
                patch.object(pdf_render, "render_workspace_pdf", render),
            ):
                resp = await public_client.get(
                    f"/api/v1/public/shared/{_SHARE_TOKEN}/files/serve/results/report.html",
                    params={"format": "pdf"},
                )
            assert resp.status_code == status, exc


# ---------------------------------------------------------------------------
# TestPublicNoSandboxWake — unauthenticated routes must never wake a sandbox
# ---------------------------------------------------------------------------

# list/read/download lazily import WorkspaceManager + FilePersistenceService
# inside the handler, so patch them at their source modules.
_FP_TREE = "src.server.services.persistence.file.FilePersistenceService.get_file_tree"
_WSMGR = "src.server.services.workspace_manager.WorkspaceManager"
_PUB_VAULT = "src.server.app.public.get_vault_secrets_for_redaction"


def _no_warm_manager():
    """A WorkspaceManager whose get_session_if_ready returns None (no warm
    session) and whose get_session_for_workspace would explode if ever called
    (it must not be)."""
    from unittest.mock import MagicMock

    mgr = MagicMock()
    mgr.get_session_if_ready.return_value = None
    mgr.get_session_for_workspace = AsyncMock(
        side_effect=AssertionError("must not wake a sandbox from a public route")
    )
    holder = MagicMock()
    holder.get_instance.return_value = mgr
    return holder, mgr


class TestPublicNoSandboxWake:
    """A 'running' DB row with no warm session must fall back to DB-only and
    never call get_session_for_workspace() (which does a Daytona attach/restart).
    Denial-of-wallet guard mirroring workspace_files._resolve_serve_bytes."""

    async def test_list_running_cold_sandbox_uses_db_not_wake(self, public_client):
        holder, mgr = _no_warm_manager()
        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="running"))),
            patch(_WORK_DIR, return_value="/home/workspace"),
            patch(_NORM_PATH, return_value=""),
            patch(_FP_TREE, AsyncMock(return_value=[])),
            patch(_WSMGR, holder),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files",
                params={"path": "."},
            )
        assert resp.status_code == 200
        assert resp.json()["files"] == []
        mgr.get_session_for_workspace.assert_not_awaited()
        mgr.get_session_if_ready.assert_called_once()

    async def test_read_running_cold_sandbox_uses_db_not_wake(self, public_client):
        holder, mgr = _no_warm_manager()
        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="running"))),
            patch(_WORK_DIR, return_value="/home/workspace"),
            patch(_NORM_PATH, return_value="data/x.txt"),
            patch(_PUB_VAULT, AsyncMock(return_value={})),
            patch(_FILE_SVC, AsyncMock(return_value=None)),
            patch(_WSMGR, holder),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/read",
                params={"path": "data/x.txt"},
            )
        assert resp.status_code == 404
        mgr.get_session_for_workspace.assert_not_awaited()

    async def test_download_running_cold_sandbox_uses_db_not_wake(self, public_client):
        holder, mgr = _no_warm_manager()
        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_DB_GET_WS, AsyncMock(return_value=_make_workspace(status="running"))),
            patch(_WORK_DIR, return_value="/home/workspace"),
            patch(_NORM_PATH, return_value="data/x.txt"),
            patch(_PUB_VAULT, AsyncMock(return_value={})),
            patch(_FILE_SVC, AsyncMock(return_value=None)),
            patch(_WSMGR, holder),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/download",
                params={"path": "data/x.txt"},
            )
        assert resp.status_code == 404
        mgr.get_session_for_workspace.assert_not_awaited()


# ---------------------------------------------------------------------------
# TestPublicReadTruncation — DB-fallback read reports real truncation state
# ---------------------------------------------------------------------------


class TestPublicReadTruncation:
    """read_shared_file's DB path must compute `truncated` from the line range,
    not hardcode False, so the UI can offer 'load more'."""

    async def test_truncated_true_when_more_lines(self, public_client):
        content = "\n".join(f"line{i}" for i in range(10))
        empty = SecretRedactor.__new__(SecretRedactor)
        empty._secrets = []
        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_DB_GET_WS, AsyncMock(return_value=_make_workspace())),
            patch(_WORK_DIR, return_value="/home/workspace"),
            patch(_NORM_PATH, return_value="data/big.txt"),
            patch(_FILE_SVC, AsyncMock(return_value=_make_file_record(content))),
            patch(_GET_REDACTOR, return_value=empty),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/read",
                params={"path": "data/big.txt", "offset": 0, "limit": 3},
            )
        assert resp.status_code == 200
        assert resp.json()["truncated"] is True

    async def test_truncated_false_when_fully_read(self, public_client):
        content = "\n".join(f"line{i}" for i in range(3))
        empty = SecretRedactor.__new__(SecretRedactor)
        empty._secrets = []
        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_DB_GET_WS, AsyncMock(return_value=_make_workspace())),
            patch(_WORK_DIR, return_value="/home/workspace"),
            patch(_NORM_PATH, return_value="data/small.txt"),
            patch(_FILE_SVC, AsyncMock(return_value=_make_file_record(content))),
            patch(_GET_REDACTOR, return_value=empty),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/files/read",
                params={"path": "data/small.txt", "offset": 0, "limit": 100},
            )
        assert resp.status_code == 200
        assert resp.json()["truncated"] is False


# ---------------------------------------------------------------------------
# TestReplayStripsWorkspaceId — workspace_id is a bearer credential
# ---------------------------------------------------------------------------

_QUERIES = "src.server.app.public.get_queries_for_thread"
_RESPONSES = "src.server.app.public.get_responses_for_thread"


def _parse_sse_events(body: str):
    """Parse an SSE text body into a list of (event, data-dict) tuples."""
    out = []
    for block in body.split("\n\n"):
        event = None
        data = None
        for line in block.splitlines():
            if line.startswith("event: "):
                event = line[len("event: ") :]
            elif line.startswith("data: "):
                import json as _json

                data = _json.loads(line[len("data: ") :])
        if event and data is not None:
            out.append((event, data))
    return out


class TestReplayStripsWorkspaceId:
    """The public replay must not leak workspace_id (the bearer credential for
    /api/v1/wsfiles/{workspace_id}/{path}) from stored workspace_status events."""

    async def test_replay_strips_workspace_id_from_stored_events(self, public_client):
        stored = {
            "turn_index": 0,
            "conversation_response_id": "resp-1",
            "sse_events": [
                {
                    "event": "workspace_status",
                    "data": {
                        "status": "ready",
                        "workspace_id": _WORKSPACE_ID,
                        "sandbox_state": "archived",
                    },
                },
                {
                    "event": "message_chunk",
                    "data": {"content": "hello"},
                },
            ],
        }
        # A deep copy guards the assertion that we never mutate the stored object.
        import copy

        original = copy.deepcopy(stored)
        query = {"turn_index": 0, "content": "hi", "created_at": "2026-01-01T00:00:00Z"}
        with (
            patch(_THREAD_BY_TOKEN, AsyncMock(return_value=_make_thread())),
            patch(_QUERIES, AsyncMock(return_value=([query], 1))),
            patch(_RESPONSES, AsyncMock(return_value=([stored], 1))),
        ):
            resp = await public_client.get(
                f"/api/v1/public/shared/{_SHARE_TOKEN}/replay"
            )
        assert resp.status_code == 200
        events = _parse_sse_events(resp.text)
        ws_status = [d for e, d in events if e == "workspace_status"]
        assert ws_status, "expected a replayed workspace_status event"
        for d in ws_status:
            assert "workspace_id" not in d
            assert "sandbox_state" not in d
        # The stored event object must be untouched (no in-place mutation).
        assert stored == original
