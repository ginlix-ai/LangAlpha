"""Tests for :mod:`src.server.services.memo_binary_storage`.

These tests mock the storage layer (``upload_bytes`` / ``get_bytes`` /
``is_storage_enabled``) — no network calls to R2 are made.
"""

from __future__ import annotations

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
            key="q1-thesis.pdf",
            content=b"%PDF-1.4 fake",
            content_type="application/pdf",
        )

    assert result is None
    upload_mock.assert_not_called()


@pytest.mark.asyncio
async def test_store_binary_uploads_and_returns_binary_ref():
    """When configured, store_binary calls upload_bytes with the expected
    key + content_type and returns the expected binary_ref shape."""
    upload_mock = MagicMock(return_value=True)
    with (
        patch.object(memo_binary_storage, "is_storage_enabled", return_value=True),
        patch.object(memo_binary_storage, "_storage_upload_bytes", upload_mock),
    ):
        ref = await store_binary(
            user_id="u1",
            key="q1-thesis.pdf",
            content=b"%PDF-1.4 bytes",
            content_type="application/pdf",
        )

    assert ref == {
        "storage": "r2",
        "key": "memo/u1/q1-thesis.pdf",
        "content_type": "application/pdf",
    }
    # Also verify the exact positional args handed to upload_bytes.
    upload_mock.assert_called_once_with(
        "memo/u1/q1-thesis.pdf",
        b"%PDF-1.4 bytes",
        "application/pdf",
    )


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
                key="q1-thesis.pdf",
                content=b"bytes",
                content_type="application/pdf",
            )

    upload_mock.assert_called_once()


# ---------------------------------------------------------------------------
# fetch_binary()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_store_and_fetch_round_trip():
    """fetch_binary reads back what store_binary wrote (mocked storage layer).

    Uses a shared in-memory dict as the fake storage to prove the key the
    adapter writes to is the same key it reads from.
    """
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
            key="q1-thesis.pdf",
            content=payload,
            content_type="application/pdf",
        )
        assert ref is not None
        assert ref["key"] == "memo/u1/q1-thesis.pdf"

        fetched = await fetch_binary(ref)

    assert fetched == payload


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
# Key format
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stored_key_pattern_matches_memo_user_slug():
    """The object-storage key must be exactly ``memo/{user_id}/{slug}``."""
    captured: dict[str, str] = {}

    def fake_upload(key: str, data: bytes, content_type: str | None = None) -> bool:
        captured["key"] = key
        captured["content_type"] = content_type or ""
        return True

    with (
        patch.object(memo_binary_storage, "is_storage_enabled", return_value=True),
        patch.object(memo_binary_storage, "_storage_upload_bytes", side_effect=fake_upload),
    ):
        await store_binary(
            user_id="u1",
            key="q1-thesis.pdf",
            content=b"x",
            content_type="application/pdf",
        )

    assert captured["key"] == "memo/u1/q1-thesis.pdf"


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
    """``_build_key`` refuses user_ids that could escape the ``memo/`` prefix.

    The check is defense-in-depth — every caller today resolves user_id via
    ``CurrentUserId`` — but the service-token auth path returns the
    ``X-User-Id`` header verbatim, so the storage layer enforces its own
    invariant rather than trusting the caller.
    """
    with patch.object(memo_binary_storage, "is_storage_enabled", return_value=True):
        with pytest.raises(MemoBinaryStorageError):
            await store_binary(
                user_id=bad_user_id,
                key="q1.pdf",
                content=b"x",
                content_type="application/pdf",
            )
