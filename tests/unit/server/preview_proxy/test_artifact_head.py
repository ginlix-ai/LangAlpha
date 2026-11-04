"""A HEAD of a version's file: GET's answer without its body, and without
reading one.

A file can be 25 MiB in object storage and a document is spliced with the
bridge on the way out, so a HEAD that fetched and spliced to learn a length
would pay a GET's whole cost to send nothing. The length a version holds was
sealed with it; a bridged document's is left out, as a HEAD may.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.server.database.artifacts import StoredVersionFile
from src.server.preview_proxy import artifacts as artifact_proxy
from src.server.services.artifacts import grants
from src.server.services.artifacts.grants import mint_artifact_grant

ARTIFACT = "11111111-2222-4333-8444-555555555555"
HTML = b"<!doctype html><html><head><title>t</title></head><body>v9</body></html>"
JSON = b'{"rows": [1, 2, 3]}'


def _stored(content_type: str, data: bytes, *, in_blob: bool) -> StoredVersionFile:
    return StoredVersionFile(
        user_id="owner-1",
        sha256="b" * 64,
        byte_len=len(data),
        content_type=content_type,
        content=None if in_blob else data,
    )


@pytest.fixture(autouse=True)
def _keys(monkeypatch):
    monkeypatch.setattr(grants._OWNER, "_key", b"o" * 32)


@pytest.fixture
def version_file(monkeypatch):
    lookup = AsyncMock()
    monkeypatch.setattr(artifact_proxy, "get_version_file", lookup)
    return lookup


@pytest.fixture
def no_bytes_read(monkeypatch):
    """A HEAD that reached for the bytes or the splice fails loudly."""
    monkeypatch.setattr(
        artifact_proxy, "fetch_blob", AsyncMock(side_effect=AssertionError("fetched"))
    )

    def splice(*_a, **_k):
        raise AssertionError("bridged")

    monkeypatch.setattr(artifact_proxy, "with_bridge", splice)


async def _token() -> str:
    return (await mint_artifact_grant(ARTIFACT)).token


async def _serve(method: str, path: str, headers=()) -> tuple[int, dict[str, str], bytes]:
    """One request through the artifact host's ASGI answer."""
    scope = {
        "type": "http",
        "method": method,
        "path": f"/g/{await _token()}/v/9/{path}",
        "headers": list(headers),
    }
    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    await artifact_proxy.serve_artifact(
        scope, None, send, artifact_id=ARTIFACT, frame_ancestors=None
    )
    start, body = sent
    headers_out = {k.decode(): v.decode() for k, v in start["headers"]}
    return start["status"], headers_out, body["body"]


@pytest.mark.asyncio
async def test_a_head_reads_no_bytes_and_states_the_sealed_length(
    version_file, no_bytes_read
):
    version_file.return_value = _stored("application/json", JSON, in_blob=True)

    status, headers, body = await _serve("HEAD", "data.json")

    assert status == 200
    assert headers["content-length"] == str(len(JSON))
    assert body == b""


@pytest.mark.asyncio
async def test_a_bridged_head_leaves_the_length_unsaid(version_file, no_bytes_read):
    version_file.return_value = _stored("text/html", HTML, in_blob=True)

    status, headers, body = await _serve("HEAD", "index.html")

    assert status == 200
    assert "content-length" not in headers
    assert body == b""


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content_type, data, path",
    [("text/html", HTML, "index.html"), ("application/json", JSON, "data.json")],
)
async def test_get_and_head_differ_only_in_the_length(
    version_file, content_type, data, path
):
    version_file.return_value = _stored(content_type, data, in_blob=False)

    _, got, body = await _serve("GET", path)
    _, head, _ = await _serve("HEAD", path)

    assert got["content-length"] == str(len(body))
    assert {k: v for k, v in got.items() if k != "content-length"} == {
        k: v for k, v in head.items() if k != "content-length"
    }


@pytest.mark.asyncio
async def test_a_head_the_browser_holds_answers_304(version_file, no_bytes_read):
    version_file.return_value = _stored("application/json", JSON, in_blob=True)
    _, first, _ = await _serve("HEAD", "data.json")

    status, headers, body = await _serve(
        "HEAD", "data.json", [(b"if-none-match", first["etag"].encode())]
    )

    assert status == 304
    assert "content-length" not in headers
    assert body == b""
