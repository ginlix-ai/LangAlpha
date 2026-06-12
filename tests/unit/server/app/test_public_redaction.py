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


# ---------------------------------------------------------------------------
# TestServeSharedFile — GET /shared/{token}/files/serve/{path}
# ---------------------------------------------------------------------------

# serve_workspace_file resolves bytes / work dir / vault secrets from the
# workspace_files module, while the share endpoint resolves the thread and
# workspace from the public module. We patch each at its source.
_PUBLIC_DB_GET_WS = "src.server.app.public.db_get_workspace"
_WS_DB_GET_WS = "src.server.app.workspace_files.db_get_workspace"
_WS_WORK_DIR = "src.server.app.workspace_files._get_work_dir"
_WS_FP = "src.server.app.workspace_files.FilePersistenceService"
_WS_VAULT = "src.server.app.workspace_files.get_vault_secrets_for_redaction"


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
        assert resp.headers["content-security-policy"] == "sandbox allow-scripts"
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
