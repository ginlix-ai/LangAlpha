"""Object-storage adapter for memo binaries (e.g. original PDF bytes).

Thin wrapper over ``src.utils.storage`` that routes memo binaries to the
configured object storage (Cloudflare R2, Tencent COS, AWS S3, MinIO...)
when one is configured, and otherwise signals the caller to fall back to
inline base64 in the store value.

The R2 public URL is intentionally NOT exposed in ``binary_ref`` — memo
downloads always stream bytes back through our own endpoint so that the
bucket can stay private and access control lives in the server layer.
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from typing import Any

from src.utils.storage import (
    delete_object as _storage_delete_object,
    get_bytes as _storage_get_bytes,
    is_storage_enabled,
    upload_bytes as _storage_upload_bytes,
)

logger = logging.getLogger(__name__)

# Storage identifier stamped into ``binary_ref``. Kept stable across providers
# (R2/S3/COS) because the adapter is what knows how to fetch it back — the
# caller never needs to vary behavior on this value.
_BINARY_STORAGE_ID = "r2"

# Defense-in-depth: refuse user_ids that could escape the ``memo/{user_id}/...``
# prefix even though every caller today resolves through ``CurrentUserId``.
# Matches a UUID, the ``LOCAL_DEV_USER_ID`` shape, and any opaque token shorter
# than 64 chars made of safe identifier characters. Reject ``/``, ``..``, and
# whitespace explicitly because an upstream auth regression could otherwise
# turn into a cross-tenant write.
_USER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class MemoBinaryStorageError(Exception):
    """Base class for memo binary storage failures."""


class MemoBinaryUploadError(MemoBinaryStorageError):
    """Raised when an upload to object storage was attempted but failed."""


class MemoBinaryFetchError(MemoBinaryStorageError):
    """Raised when fetching a memo binary from object storage failed."""


def is_configured() -> bool:
    """Return True iff an object-storage backend is usable for memo binaries.

    Every supported provider (R2, S3, COS, Alibaba OSS) now exposes
    ``upload_bytes`` + ``get_bytes`` + ``delete_object``, so any enabled
    backend can round-trip memo binaries.

    When False, ``store_binary`` is a no-op (returns None) and callers must
    fall back to inline base64 in the store value.
    """
    return is_storage_enabled()


_MIME_EXTENSION = {
    "application/pdf": ".pdf",
}


def _validate_user_id(user_id: str) -> None:
    """Refuse user_ids that could escape the ``memo/{user_id}/...`` prefix."""
    if not _USER_ID_RE.match(user_id):
        msg = f"Refusing to build storage key for unsafe user_id: {user_id!r}"
        raise MemoBinaryStorageError(msg)


def _build_storage_key(user_id: str, content_type: str) -> str:
    """Build a fresh ``memo/{user_id}/{uuid}{ext}`` storage key.

    UUID-based so concurrent uploads to the same slug don't collide on bytes.
    """
    _validate_user_id(user_id)
    ext = _MIME_EXTENSION.get(content_type, "")
    return f"memo/{user_id}/{uuid.uuid4().hex}{ext}"


async def store_binary(
    *,
    user_id: str,
    content: bytes,
    content_type: str,
) -> dict[str, Any] | None:
    """Upload memo bytes to a fresh UUID storage key.

    Returns the ``binary_ref`` dict on success, ``None`` when storage is not
    configured. Raises :class:`MemoBinaryUploadError` when configured but the
    upload failed.
    """
    if not is_configured():
        return None

    storage_key = _build_storage_key(user_id, content_type)
    # upload_bytes is a synchronous boto3 call; off-load to a thread so we
    # don't block the event loop (same pattern as persistence/image_capture).
    success = await asyncio.to_thread(
        _storage_upload_bytes, storage_key, content, content_type,
    )
    if not success:
        logger.error(
            "Failed to upload memo binary to object storage (user=%s storage_key=%s)",
            user_id,
            storage_key,
        )
        msg = (
            "Could not store the original file in object storage. Please retry."
        )
        raise MemoBinaryUploadError(msg)

    return {
        "storage": _BINARY_STORAGE_ID,
        "key": storage_key,
        "content_type": content_type,
    }


async def delete_binary(binary_ref: dict[str, Any]) -> bool:
    """Best-effort delete of a memo binary.

    The caller should treat the result as advisory: a failure leaves an
    orphan object in the bucket, which is harmless from a correctness
    standpoint (the store value referencing it is gone) but matters for
    storage hygiene and right-to-erasure compliance.

    Returns:
        ``True`` when the delete succeeded, ``False`` when the binary_ref
        was malformed, storage is not configured, or the underlying delete
        call failed. Never raises — the upload/write flow already handles
        the soft-error case.
    """
    if not isinstance(binary_ref, dict):
        return False
    storage_key = binary_ref.get("key")
    if not storage_key or not isinstance(storage_key, str):
        return False
    if not is_configured():
        return False
    try:
        return bool(
            await asyncio.to_thread(_storage_delete_object, storage_key)
        )
    except Exception:
        logger.exception(
            "memo binary delete failed (orphan left behind)",
            extra={"storage_key": storage_key},
        )
        return False


async def fetch_binary(binary_ref: dict[str, Any]) -> bytes:
    """Fetch the bytes referenced by a ``binary_ref``.

    Raises:
        MemoBinaryFetchError: If the ref is malformed, the storage backend
            is no longer configured, or the object could not be downloaded.
    """
    if not isinstance(binary_ref, dict):
        msg = "binary_ref must be a dict"
        raise MemoBinaryFetchError(msg)

    storage_key = binary_ref.get("key")
    if not storage_key or not isinstance(storage_key, str):
        msg = "binary_ref is missing a valid 'key'"
        raise MemoBinaryFetchError(msg)

    if not is_configured():
        msg = (
            "Object storage is not configured; cannot fetch memo binary "
            f"at {storage_key}"
        )
        raise MemoBinaryFetchError(msg)

    data = await asyncio.to_thread(_storage_get_bytes, storage_key)
    if data is None:
        msg = f"Memo binary not found or unreadable at {storage_key}"
        raise MemoBinaryFetchError(msg)
    return data
