"""Integration tests for secret redaction in workspace file endpoints.

Writes files containing secret values to a real sandbox (MemoryProvider),
then verifies that read and download endpoints redact them.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.server.utils.secret_redactor import SecretRedactor

from .conftest import TEST_WS_ID

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

BASE = f"/api/v1/workspaces/{TEST_WS_ID}/files"

# A fake secret that will be "known" to the redactor
_SECRET_NAME = "FMP_API_KEY"
_SECRET_VALUE = "sk_test_fmp_1234567890abcdef"


@pytest.fixture
def mock_redactor():
    """A SecretRedactor pre-loaded with a known secret (bypasses config)."""
    r = SecretRedactor.__new__(SecretRedactor)
    r._secrets = [(_SECRET_NAME, _SECRET_VALUE)]
    return r


# ---------------------------------------------------------------------------
# TestReadFileRedaction
# ---------------------------------------------------------------------------


class TestReadFileRedaction:
    """Verify read_workspace_file redacts secrets from live sandbox content."""

    async def test_read_redacts_secret_from_sandbox(self, files_client, mock_redactor):
        client, sandbox = files_client

        # Write a file containing the secret to the sandbox
        content = f"API_KEY={_SECRET_VALUE}\nother_data=safe"
        await sandbox.aupload_file_bytes(
            f"{sandbox._work_dir}/data/env_leak.txt", content.encode()
        )

        with patch("src.server.app.workspace_files.crud.get_redactor", return_value=mock_redactor):
            resp = await client.get(f"{BASE}/read", params={"path": "data/env_leak.txt"})

        assert resp.status_code == 200
        body = resp.json()

        # Secret must be redacted
        assert _SECRET_VALUE not in body["content"]
        assert f"[REDACTED:{_SECRET_NAME}]" in body["content"]

        # Non-secret content must be preserved
        assert "other_data=safe" in body["content"]

    async def test_read_no_redaction_when_no_secrets(self, files_client):
        """When redactor has no secrets, content passes through unchanged."""
        client, sandbox = files_client

        content = "just normal data"
        await sandbox.aupload_file_bytes(
            f"{sandbox._work_dir}/data/normal.txt", content.encode()
        )

        empty_redactor = SecretRedactor.__new__(SecretRedactor)
        empty_redactor._secrets = []

        with patch("src.server.app.workspace_files.crud.get_redactor", return_value=empty_redactor):
            resp = await client.get(f"{BASE}/read", params={"path": "data/normal.txt"})

        assert resp.status_code == 200
        assert resp.json()["content"] == content


# ---------------------------------------------------------------------------
# TestDownloadFileRedaction
# ---------------------------------------------------------------------------


class TestDownloadFileRedaction:
    """Verify download_workspace_file redacts secrets from text files."""

    async def test_download_redacts_secret_from_text_file(self, files_client, mock_redactor):
        client, sandbox = files_client

        content = f"secret={_SECRET_VALUE}"
        await sandbox.aupload_file_bytes(
            f"{sandbox._work_dir}/data/config.txt", content.encode()
        )

        with patch("src.server.app.workspace_files.crud.get_redactor", return_value=mock_redactor):
            resp = await client.get(f"{BASE}/download", params={"path": "data/config.txt"})

        assert resp.status_code == 200
        body = resp.content.decode("utf-8")

        assert _SECRET_VALUE not in body
        assert f"[REDACTED:{_SECRET_NAME}]" in body

    async def test_download_skips_redaction_for_binary(self, files_client, mock_redactor):
        """Binary files (e.g., PNG) are not redacted."""
        client, sandbox = files_client

        # Upload a file with .png extension — redaction should be skipped
        # even if the bytes happen to contain the secret string
        binary_with_secret = b"\x89PNG" + _SECRET_VALUE.encode()
        await sandbox.aupload_file_bytes(
            f"{sandbox._work_dir}/data/chart.png", binary_with_secret
        )

        with patch("src.server.app.workspace_files.crud.get_redactor", return_value=mock_redactor):
            resp = await client.get(f"{BASE}/download", params={"path": "data/chart.png"})

        assert resp.status_code == 200
        # Binary content should NOT be redacted (MIME is image/png, not text/*)
        assert _SECRET_VALUE.encode() in resp.content
