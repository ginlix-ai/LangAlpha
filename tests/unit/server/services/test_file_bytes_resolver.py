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
    resolve_file_byte_range,
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


# --- ranged reads ------------------------------------------------------------

_RANGED = "src.server.services.persistence.resolve.fetch_blob_range"
_WHOLE = "src.server.services.persistence.resolve.fetch_blob"
PACK = hashlib.sha256(b"pack chunk").hexdigest()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [
        {"content_binary": b"0123456789"},
        {"content_binary": memoryview(b"0123456789")},
        {"content_text": "0123456789"},
    ],
)
async def test_an_inline_row_is_never_ranged(row):
    """Its bytes come with the row, so a slice would load the whole file for
    every range; the caller answers it whole once instead."""
    row = {**row, "file_size": 10, "blob_sha256": SHA}
    with patch(_RANGED, new=AsyncMock()) as ranged:
        assert await resolve_file_byte_range(row, 2, 4, user_id=USER) is None
    ranged.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_blob_row_reads_only_the_slice_without_the_digest_check():
    """A part of the file cannot be checked against the file's hash, so none
    is asked for; the whole object is never fetched."""
    row = {"blob_sha256": SHA, "file_size": 1000, "content_hash": SHA}
    with (
        patch(_RANGED, new=AsyncMock(return_value=b"x" * 100)) as ranged,
        patch(_WHOLE, new=AsyncMock()) as whole,
    ):
        assert await resolve_file_byte_range(row, 900, 4096, user_id=USER) == (
            b"x" * 100,
            1000,
        )
    ranged.assert_awaited_once_with(USER, SHA, 900, 100, expected_sha256=None)
    whole.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_pack_member_reads_at_its_offset_in_the_chunk():
    row = {
        "pack_sha256": PACK,
        "pack_offset": 5000,
        "file_size": 50,
        "content_hash": SHA,
    }
    with patch(_RANGED, new=AsyncMock(return_value=b"abcde")) as ranged:
        assert await resolve_file_byte_range(row, 10, 5, user_id=USER) == (b"abcde", 50)
    ranged.assert_awaited_once_with(USER, PACK, 5010, 5, expected_sha256=None)


_STORAGE = "src.server.database.workspace_file_blobs._storage_get_bytes_range"
WHOLE_FILE = b"%PDF-1.7 a whole stored file"
WHOLE_SHA = hashlib.sha256(WHOLE_FILE).hexdigest()


def _whole_file_row(kind: str) -> dict:
    if kind == "blob":
        return {
            "blob_sha256": WHOLE_SHA,
            "content_hash": WHOLE_SHA,
            "file_size": len(WHOLE_FILE),
        }
    return {
        "pack_sha256": PACK,
        "pack_offset": 5000,
        "content_hash": WHOLE_SHA,
        "file_size": len(WHOLE_FILE),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["blob", "pack"])
async def test_a_slice_that_is_the_whole_file_is_checked_against_its_digest(kind):
    """It is the file's bytes, so a mis-keyed object or a wrong pack offset is
    as loud as it is on a whole read, where it would otherwise serve wrong."""
    row = _whole_file_row(kind)
    base = int(row.get("pack_offset") or 0)

    def stored(data: bytes):
        return lambda _key, start, n: data[start - base : start - base + n]

    # Clamped to the file's end, a longer range is the whole file too.
    with patch(_STORAGE, side_effect=stored(WHOLE_FILE)):
        assert await resolve_file_byte_range(row, 0, 4096, user_id=USER) == (
            WHOLE_FILE,
            len(WHOLE_FILE),
        )
    tampered = WHOLE_FILE.replace(b"whole", b"wrong")
    with patch(_STORAGE, side_effect=stored(tampered)):
        with pytest.raises(FileBytesUnavailable, match="hash mismatch"):
            await resolve_file_byte_range(row, 0, 4096, user_id=USER)
        # A part of it has nothing to be checked against.
        assert await resolve_file_byte_range(row, 0, 5, user_id=USER) == (
            tampered[:5],
            len(WHOLE_FILE),
        )


@pytest.mark.asyncio
async def test_a_stored_range_past_the_end_reads_nothing():
    with patch(_RANGED, new=AsyncMock()) as ranged:
        row = {"blob_sha256": SHA, "file_size": 1000}
        assert await resolve_file_byte_range(row, 1000, 10, user_id=USER) == (b"", 1000)
    ranged.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [
        {"blob_sha256": SHA, "file_size": None},
        {"content_text": None, "content_binary": None, "file_size": 10},
    ],
)
async def test_a_row_that_cannot_be_ranged_is_none(row):
    """No size to measure a range against, or no content at all: the caller
    answers whole, which is where an empty row gets its 404."""
    assert await resolve_file_byte_range(row, 0, 10, user_id=USER) is None


@pytest.mark.asyncio
async def test_a_failed_ranged_read_is_unavailable_not_absent():
    with patch(_RANGED, new=AsyncMock(side_effect=BlobFetchError("storage down"))):
        with pytest.raises(FileBytesUnavailable):
            await resolve_file_byte_range(
                {"blob_sha256": SHA, "file_size": 1000}, 0, 10, user_id=USER
            )
