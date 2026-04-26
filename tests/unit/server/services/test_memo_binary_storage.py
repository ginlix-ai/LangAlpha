"""Tests for :mod:`src.server.services.memo_binary_storage`.

These tests mock the storage layer (``upload_bytes`` / ``get_bytes`` /
``is_storage_enabled``) — no network calls to R2 are made.
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock, patch

import pytest

from src.server.services import memo_binary_storage
from src.server.services.memo_binary_storage import (
    MemoBinaryFetchError,
    MemoBinaryStorageError,
    MemoBinaryUploadError,
    fetch_binary,
    is_configured,
    store_binary,
)


_UUID_PDF_RE = re.compile(r"^memo/[A-Za-z0-9_-]+/[0-9a-f]{32}\.pdf$")


# ---------------------------------------------------------------------------
# is_configured()
# ---------------------------------------------------------------------------


def test_is_configured_false_when_storage_disabled():
    """When object storage is not configured, is_configured() returns False."""
    with patch.object(memo_binary_storage, "is_storage_enabled", return_value=False):
        assert is_configured() is False


def test_is_configured_true_when_storage_enabled():
    """When object storage is configured, is_configured() returns True."""
    with patch.object(memo_binary_storage, "is_storage_enabled", return_value=True):
        assert is_configured() is True


# ---------------------------------------------------------------------------
# store_binary()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_store_binary_returns_none_when_not_configured():
    """Without object storage, store_binary is a no-op — no upload attempted."""
    upload_mock = MagicMock(return_value=True)
    with (
        patch.object(memo_binary_storage, "is_storage_enabled", return_value=False),
        patch.object(memo_binary_storage, "_storage_upload_bytes", upload_mock),
    ):
        result = await store_binary(
            user_id="u1",
            content=b"%PDF-1.4 fake",
            content_type="application/pdf",
        )

    assert result is None
    upload_mock.assert_not_called()


@pytest.mark.asyncio
async def test_store_binary_uploads_and_returns_uuid_keyed_ref():
    """Uploads land at ``memo/{user}/{uuid}.pdf`` — opaque, slug-independent."""
    upload_mock = MagicMock(return_value=True)
    with (
        patch.object(memo_binary_storage, "is_storage_enabled", return_value=True),
        patch.object(memo_binary_storage, "_storage_upload_bytes", upload_mock),
    ):
        ref = await store_binary(
            user_id="u1",
            content=b"%PDF-1.4 bytes",
            content_type="application/pdf",
        )

    assert ref is not None
    assert ref["storage"] == "r2"
    assert ref["content_type"] == "application/pdf"
    assert _UUID_PDF_RE.match(ref["key"]), ref["key"]
    upload_mock.assert_called_once()
    storage_key, body, content_type = upload_mock.call_args.args
    assert storage_key == ref["key"]
    assert body == b"%PDF-1.4 bytes"
    assert content_type == "application/pdf"


@pytest.mark.asyncio
async def test_store_binary_two_calls_produce_distinct_keys():
    """Two uploads of identical content go to distinct UUIDs — no overwrite."""
    upload_mock = MagicMock(return_value=True)
    with (
        patch.object(memo_binary_storage, "is_storage_enabled", return_value=True),
        patch.object(memo_binary_storage, "_storage_upload_bytes", upload_mock),
    ):
        ref_a = await store_binary(
            user_id="u1", content=b"x", content_type="application/pdf",
        )
        ref_b = await store_binary(
            user_id="u1", content=b"x", content_type="application/pdf",
        )

    assert ref_a is not None and ref_b is not None
    assert ref_a["key"] != ref_b["key"]


@pytest.mark.asyncio
async def test_store_binary_raises_upload_error_on_storage_failure():
    """R2 outage: upload_bytes returns False → MemoBinaryUploadError."""
    upload_mock = MagicMock(return_value=False)
    with (
        patch.object(memo_binary_storage, "is_storage_enabled", return_value=True),
        patch.object(memo_binary_storage, "_storage_upload_bytes", upload_mock),
    ):
        with pytest.raises(MemoBinaryUploadError):
            await store_binary(
                user_id="u1",
                content=b"bytes",
                content_type="application/pdf",
            )

    upload_mock.assert_called_once()


# ---------------------------------------------------------------------------
# fetch_binary()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_store_and_fetch_round_trip():
    """fetch_binary reads back what store_binary wrote (mocked storage layer)."""
    fake_storage: dict[str, bytes] = {}

    def fake_upload(key: str, data: bytes, content_type: str | None = None) -> bool:
        fake_storage[key] = data
        return True

    def fake_get(key: str) -> bytes | None:
        return fake_storage.get(key)

    payload = b"%PDF-1.4 round-trip"

    with (
        patch.object(memo_binary_storage, "is_storage_enabled", return_value=True),
        patch.object(memo_binary_storage, "_storage_upload_bytes", side_effect=fake_upload),
        patch.object(memo_binary_storage, "_storage_get_bytes", side_effect=fake_get),
    ):
        ref = await store_binary(
            user_id="u1",
            content=payload,
            content_type="application/pdf",
        )
        assert ref is not None

        fetched = await fetch_binary(ref)

    assert fetched == payload


@pytest.mark.asyncio
async def test_fetch_binary_supports_legacy_slug_keys():
    """Existing rows with slug-derived ``binary_ref.key`` still download.

    Legacy uploads stored at ``memo/{user}/{slug}.pdf`` (e.g.
    ``memo/u1/q1-thesis.pdf``). The key is opaque to fetch_binary, so any
    string the storage layer can resolve still works.
    """
    legacy_ref = {
        "storage": "r2",
        "key": "memo/u1/legacy-slug.pdf",
        "content_type": "application/pdf",
    }
    with (
        patch.object(memo_binary_storage, "is_storage_enabled", return_value=True),
        patch.object(
            memo_binary_storage,
            "_storage_get_bytes",
            return_value=b"legacy bytes",
        ),
    ):
        assert await fetch_binary(legacy_ref) == b"legacy bytes"


@pytest.mark.asyncio
async def test_fetch_binary_raises_when_not_configured():
    """If object storage has been torn down, fetch_binary surfaces the error."""
    with patch.object(memo_binary_storage, "is_storage_enabled", return_value=False):
        with pytest.raises(MemoBinaryFetchError):
            await fetch_binary({"storage": "r2", "key": "memo/u1/q1-thesis.pdf"})


@pytest.mark.asyncio
async def test_fetch_binary_raises_on_missing_object():
    """When the storage layer returns None, fetch_binary raises a clear error."""
    with (
        patch.object(memo_binary_storage, "is_storage_enabled", return_value=True),
        patch.object(memo_binary_storage, "_storage_get_bytes", return_value=None),
    ):
        with pytest.raises(MemoBinaryFetchError):
            await fetch_binary({"storage": "r2", "key": "memo/u1/missing.pdf"})


@pytest.mark.asyncio
async def test_fetch_binary_raises_on_malformed_ref():
    """Missing/invalid 'key' in binary_ref is a programming bug → clear error."""
    with patch.object(memo_binary_storage, "is_storage_enabled", return_value=True):
        with pytest.raises(MemoBinaryFetchError):
            await fetch_binary({"storage": "r2"})  # no key
        with pytest.raises(MemoBinaryFetchError):
            await fetch_binary("not-a-dict")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# user_id defense-in-depth
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_user_id",
    [
        "../escape",
        "/etc/passwd",
        "user/with/slash",
        "user with spaces",
        "",
        "x" * 65,  # exceeds the 64-char cap
        "u\nl",    # newline injection
    ],
)
async def test_store_binary_rejects_unsafe_user_id(bad_user_id):
    """Refuse user_ids that could escape the ``memo/`` prefix."""
    with patch.object(memo_binary_storage, "is_storage_enabled", return_value=True):
        with pytest.raises(MemoBinaryStorageError):
            await store_binary(
                user_id=bad_user_id,
                content=b"x",
                content_type="application/pdf",
            )
