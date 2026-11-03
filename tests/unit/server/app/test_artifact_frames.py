"""A version's files on the API origin answer HEAD as the artifact host does.

Starlette states an empty body's length unless told one, which for a bridged
document's HEAD would say 0 where the GET sends the whole page.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.server.app import artifact_frames
from src.server.database.artifacts import StoredVersionFile
from src.server.preview_proxy import artifacts as artifact_proxy
from src.server.services.artifacts import grants
from src.server.services.artifacts.grants import mint_artifact_grant

ARTIFACT = "11111111-2222-4333-8444-555555555555"
HTML = b"<!doctype html><html><head><title>t</title></head><body>v9</body></html>"
JSON = b'{"rows": [1, 2, 3]}'


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(grants._OWNER, "_key", b"o" * 32)
    monkeypatch.setattr(artifact_frames, "preview_base", lambda: None)
    app = FastAPI()
    app.include_router(artifact_frames.frame_router)
    return TestClient(app)


def _version_file(monkeypatch, content_type: str, data: bytes) -> None:
    stored = StoredVersionFile(
        user_id="owner-1",
        sha256="c" * 64,
        byte_len=len(data),
        content_type=content_type,
        content=None,
    )
    monkeypatch.setattr(
        artifact_proxy, "get_version_file", AsyncMock(return_value=stored)
    )
    monkeypatch.setattr(artifact_proxy, "fetch_blob", AsyncMock(return_value=data))


async def _url(path: str) -> str:
    token = (await mint_artifact_grant(ARTIFACT)).token
    return f"/api/v1/artifacts/g/{token}/v/9/{path}"


@pytest.mark.asyncio
async def test_a_head_states_the_sealed_length_without_reading(monkeypatch, client):
    _version_file(monkeypatch, "application/json", JSON)
    url = await _url("data.json")

    head = client.head(url)

    assert head.status_code == 200
    assert head.headers["content-length"] == str(len(JSON))
    artifact_proxy.fetch_blob.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_bridged_head_leaves_the_length_unsaid(monkeypatch, client):
    _version_file(monkeypatch, "text/html", HTML)
    url = await _url("index.html")

    head = client.head(url)
    got = client.get(url)

    assert head.status_code == 200
    assert "content-length" not in head.headers
    assert got.headers["content-length"] == str(len(got.content))
    assert len(got.content) > len(HTML)
