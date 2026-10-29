"""Integration tests for workspace files API endpoints.

Each test wires a real PTCSandbox (MemoryProvider) to the FastAPI router
via the ``files_client`` fixture. Database and auth are mocked.
"""

from __future__ import annotations

import pytest

from .conftest import TEST_PROJECT, TEST_WS_ID

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE = f"/api/v1/workspaces/{TEST_WS_ID}/files"


# ---------------------------------------------------------------------------
# TestListFiles
# ---------------------------------------------------------------------------


class TestListFiles:
    async def test_list_files_running_workspace(self, files_client):
        client, sandbox = files_client

        # Seed a file into the sandbox
        await sandbox.aupload_file_bytes(
            f"{sandbox.workspace(TEST_PROJECT).workspace}/results/test.txt", b"hello"
        )

        resp = await client.get(BASE, params={"path": ".", "wait_for_sandbox": "true"})
        assert resp.status_code == 200

        body = resp.json()
        assert body["sandbox_ready"] is True
        assert "results/test.txt" in body["files"]

    async def test_list_files_hides_internal(self, files_client):
        client, sandbox = files_client

        # Seed a file inside the hidden _internal directory
        await sandbox.aupload_file_bytes(
            f"{sandbox._work_dir}/_internal/secret.txt", b"hidden"
        )

        resp = await client.get(BASE, params={"path": ".", "wait_for_sandbox": "true"})
        assert resp.status_code == 200

        body = resp.json()
        files = body["files"]
        # _internal/ contents must NOT appear in default listing
        assert "_internal/secret.txt" not in files
        assert all("_internal" not in f for f in files)


# ---------------------------------------------------------------------------
# TestReadFile
# ---------------------------------------------------------------------------


class TestReadFile:
    async def test_read_text_file(self, files_client):
        client, sandbox = files_client

        content = "hello world"
        await sandbox.aupload_file_bytes(
            f"{sandbox.workspace(TEST_PROJECT).workspace}/data/hello.txt", content.encode()
        )

        resp = await client.get(f"{BASE}/read", params={"path": "data/hello.txt"})
        assert resp.status_code == 200

        body = resp.json()
        assert body["content"] == content
        assert body["path"] == "data/hello.txt"

    async def test_read_with_offset_limit(self, files_client):
        client, sandbox = files_client

        lines = [f"line {i}" for i in range(20)]
        text = "\n".join(lines)
        await sandbox.aupload_file_bytes(
            f"{sandbox.workspace(TEST_PROJECT).workspace}/data/multiline.txt", text.encode()
        )

        resp = await client.get(
            f"{BASE}/read",
            params={"path": "data/multiline.txt", "offset": 5, "limit": 3},
        )
        assert resp.status_code == 200

        body = resp.json()
        returned_lines = body["content"].split("\n")
        assert returned_lines == ["line 5", "line 6", "line 7"]

    async def test_read_binary_returns_415(self, files_client):
        client, sandbox = files_client

        # Upload a fake PNG (just needs the extension to trigger binary check)
        await sandbox.aupload_file_bytes(
            f"{sandbox.workspace(TEST_PROJECT).workspace}/data/image.png", b"\x89PNG fake data"
        )

        resp = await client.get(f"{BASE}/read", params={"path": "data/image.png"})
        assert resp.status_code == 415
        assert "binary" in resp.json()["detail"].lower()

    async def test_read_nonexistent_returns_404(self, files_client):
        client, _sandbox = files_client

        resp = await client.get(
            f"{BASE}/read", params={"path": "data/does_not_exist.txt"}
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# TestWriteFile
# ---------------------------------------------------------------------------


class TestWriteFile:
    async def test_write_text_file(self, files_client):
        client, sandbox = files_client

        resp = await client.put(
            f"{BASE}/write",
            params={"path": "data/new.txt"},
            json={"content": "written via API"},
        )
        assert resp.status_code == 200

        body = resp.json()
        assert body["path"] == "data/new.txt"

        # Verify the file landed in the sandbox
        actual = await sandbox.aread_file_text(f"{sandbox.workspace(TEST_PROJECT).workspace}/data/new.txt")
        assert actual == "written via API"


# ---------------------------------------------------------------------------
# TestDownloadFile
# ---------------------------------------------------------------------------


class TestDownloadFile:
    async def test_download_text_file(self, files_client):
        client, sandbox = files_client

        payload = b"download me"
        await sandbox.aupload_file_bytes(
            f"{sandbox.workspace(TEST_PROJECT).workspace}/results/dl.txt", payload
        )

        resp = await client.get(
            f"{BASE}/download", params={"path": "results/dl.txt"}
        )
        assert resp.status_code == 200
        assert resp.content == payload

    async def test_download_image_cache_headers(self, files_client):
        client, sandbox = files_client

        await sandbox.aupload_file_bytes(
            f"{sandbox.workspace(TEST_PROJECT).workspace}/results/chart.png", b"\x89PNG fake"
        )

        resp = await client.get(
            f"{BASE}/download", params={"path": "results/chart.png"}
        )
        assert resp.status_code == 200
        assert resp.headers["cache-control"] == "private, max-age=300"
        assert "etag" in resp.headers

    async def test_download_etag_304(self, files_client):
        client, sandbox = files_client

        await sandbox.aupload_file_bytes(
            f"{sandbox.workspace(TEST_PROJECT).workspace}/results/pic.png", b"\x89PNG etag test"
        )

        # First request -- get the ETag
        resp1 = await client.get(
            f"{BASE}/download", params={"path": "results/pic.png"}
        )
        assert resp1.status_code == 200
        etag = resp1.headers["etag"]

        # Second request with If-None-Match -- should 304
        resp2 = await client.get(
            f"{BASE}/download",
            params={"path": "results/pic.png"},
            headers={"If-None-Match": etag},
        )
        assert resp2.status_code == 304


# ---------------------------------------------------------------------------
# TestUploadFile
# ---------------------------------------------------------------------------


class TestUploadFile:
    async def test_upload_multipart(self, files_client):
        client, sandbox = files_client

        file_content = b"uploaded via multipart"
        resp = await client.post(
            f"{BASE}/upload",
            params={"path": "data/uploaded.txt"},
            files={"file": ("uploaded.txt", file_content, "text/plain")},
        )
        assert resp.status_code == 200

        body = resp.json()
        assert body["path"] == "data/uploaded.txt"
        assert body["size"] == len(file_content)

        # Verify file in sandbox
        actual = await sandbox.adownload_file_bytes(
            f"{sandbox.workspace(TEST_PROJECT).workspace}/data/uploaded.txt"
        )
        assert actual == file_content


# ---------------------------------------------------------------------------
# TestDeleteFiles
# ---------------------------------------------------------------------------


class TestDeleteFiles:
    async def test_delete_files(self, files_client):
        client, sandbox = files_client

        await sandbox.aupload_file_bytes(
            f"{sandbox.workspace(TEST_PROJECT).workspace}/data/del.txt", b"delete me"
        )

        resp = await client.request(
            "DELETE",
            BASE,
            json={"paths": ["data/del.txt"]},
        )
        assert resp.status_code == 200

        body = resp.json()
        assert "data/del.txt" in body["deleted"]

    async def test_delete_system_path_rejected(self, files_client):
        client, sandbox = files_client
        protected_file = f"{sandbox.workspace(TEST_PROJECT).workspace}/.agents/tools/module.py"
        await sandbox.runtime.upload_file(b"protected", protected_file)

        resp = await client.request(
            "DELETE",
            BASE,
            json={"paths": [".agents/tools/module.py"]},
        )
        assert resp.status_code == 200

        body = resp.json()
        assert body["deleted"] == []
        assert len(body["errors"]) == 1
        assert "system" in body["errors"][0]["detail"].lower()
        assert await sandbox.adownload_file_bytes(protected_file) == b"protected"
