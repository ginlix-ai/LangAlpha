"""Contracts for the single byte-resolution ladder shared by every read path.

This replaced four copy-pasted ladders (restore, authenticated read/download,
unauthenticated serve, share-token routes) that had drifted apart. The cases
below are the ones the copies disagreed on.
"""

from __future__ import annotations

import hashlib
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.persistence.resolve import (
    FileBytesUnavailable,
    resolve_file_bytes,
    resolve_file_bytes_or_none,
    resolve_file_text_or_none,
)
from src.server.database.workspace_file_blobs import BlobFetchError

SHA = hashlib.sha256(b"blob bytes").hexdigest()
USER = "user-a"


@pytest.mark.asyncio
async def test_inline_text_row():
    assert await resolve_file_bytes({"content_text": "hi", "is_binary": False}, user_id=USER) == b"hi"


@pytest.mark.asyncio
async def test_inline_binary_row():
    assert (
        await resolve_file_bytes({"content_binary": b"\x00\x01", "is_binary": True}, user_id=USER)
        == b"\x00\x01"
    )


@pytest.mark.asyncio
async def test_memoryview_is_materialized():
    """psycopg hands BYTEA back as a memoryview; callers hash and len() it."""
    out = await resolve_file_bytes(
        {"content_binary": memoryview(b"\x00\x01"), "is_binary": True}, user_id=USER
    )
    assert out == b"\x00\x01"
    assert isinstance(out, bytes)


@pytest.mark.asyncio
async def test_empty_binary_is_content_not_absence():
    """The pre-existing truthiness bug: `if content_binary` 404s an empty file.

    Under blobs it was worse — the row would fall through to a pointless network
    fetch for zero bytes.
    """
    assert await resolve_file_bytes({"content_binary": b"", "is_binary": True}, user_id=USER) == b""


@pytest.mark.asyncio
async def test_empty_text_is_content_not_absence():
    assert await resolve_file_bytes({"content_text": "", "is_binary": False}, user_id=USER) == b""


@pytest.mark.asyncio
async def test_blob_row_fetches_by_pointer():
    with patch(
        "src.server.services.persistence.resolve.fetch_blob",
        new=AsyncMock(return_value=b"blob bytes"),
    ) as f:
        row = {"content_text": None, "content_binary": None, "blob_sha256": SHA}
        assert await resolve_file_bytes(row, user_id=USER) == b"blob bytes"
    f.assert_awaited_once_with(USER, SHA)


@pytest.mark.asyncio
async def test_inline_wins_over_blob_pointer():
    """A row that still carries bytes is served without touching the network."""
    with patch(
        "src.server.services.persistence.resolve.fetch_blob", new=AsyncMock()
    ) as f:
        row = {"content_text": "inline", "blob_sha256": SHA}
        assert await resolve_file_bytes(row, user_id=USER) == b"inline"
    f.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_content_anywhere_is_none():
    """Genuinely absent — distinct from unavailable, so callers can 404."""
    assert await resolve_file_bytes({"content_text": None, "blob_sha256": None}, user_id=USER) is None


@pytest.mark.asyncio
async def test_missing_keys_do_not_raise():
    """Sparse records (older fixtures, metadata-only selects) flow through."""
    assert await resolve_file_bytes({"file_path": "a.txt"}, user_id=USER) is None


@pytest.mark.asyncio
async def test_blob_failure_is_unavailable_not_absent():
    """A storage blip must not be reported as 'your file does not exist'."""
    with patch(
        "src.server.services.persistence.resolve.fetch_blob",
        new=AsyncMock(side_effect=BlobFetchError("storage down")),
    ):
        with pytest.raises(FileBytesUnavailable):
            await resolve_file_bytes({"blob_sha256": SHA}, user_id=USER)


@pytest.mark.asyncio
async def test_or_none_collapses_unavailable_to_absent():
    """The uniform-absent policy for routes whose only credential is the URL.

    ``wsfiles`` and the share-token endpoints authenticate nothing beyond an
    opaque id, so a 503 for "storage is down" next to a 404 for "no such file"
    would confirm to a guesser that the workspace and path exist.
    """
    with patch(
        "src.server.services.persistence.resolve.fetch_blob",
        new=AsyncMock(side_effect=BlobFetchError("storage down")),
    ):
        assert (
            await resolve_file_bytes_or_none({"blob_sha256": SHA}, user_id=USER, context="t") is None
        )
        assert (
            await resolve_file_text_or_none({"blob_sha256": SHA}, user_id=USER, context="t") is None
        )


@pytest.mark.asyncio
async def test_or_none_text_decodes_utf8():
    """The text routes must never hand redact() a None from
    `.get("content_text", "")` — the key exists with a NULL value."""
    assert await resolve_file_text_or_none({"content_text": "股票"}, user_id=USER, context="t") == "股票"
    assert (
        await resolve_file_text_or_none(
            {"content_text": None, "blob_sha256": None}, user_id=USER, context="t"
        )
        is None
    )


@pytest.mark.asyncio
async def test_or_none_still_returns_real_content():
    """Suppressing the error must not suppress an empty-but-present file."""
    with patch(
        "src.server.services.persistence.resolve.fetch_blob",
        new=AsyncMock(return_value=b""),
    ):
        assert await resolve_file_bytes_or_none({"blob_sha256": SHA}, user_id=USER, context="t") == b""
