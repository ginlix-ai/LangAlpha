"""Object-storage adapter for user-skill archives (canonical zip bytes).

Same shape as ``memo_binary_storage``: a thin wrapper over ``src.utils.storage``
that routes archives to the configured object storage when one exists, and
otherwise signals the caller to fall back to an inline blob on the row.

Keys are content-addressed rather than UUID-based (the one deliberate
difference from the memo adapter): the canonical re-zip makes identical skill
content produce identical bytes, so re-uploading an unchanged skill resolves to
the same key and the PUT is idempotent.
"""

from __future__ import annotations

import asyncio
import logging
import re

from src.utils.storage import (
    delete_object as _storage_delete_object,
    get_bytes as _storage_get_bytes,
    is_storage_enabled,
    upload_bytes as _storage_upload_bytes,
)

logger = logging.getLogger(__name__)

_ARCHIVE_CONTENT_TYPE = "application/zip"

# Defense-in-depth: refuse user_ids that could escape the
# ``user-skills/{user_id}/...`` prefix even though every caller resolves
# through ``CurrentUserId``. Rejects ``/``, ``..`` and whitespace explicitly.
_USER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_CONTENT_HASH_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class SkillArchiveStorageError(Exception):
    """Base class for skill archive storage failures."""


class SkillArchiveUploadError(SkillArchiveStorageError):
    """Raised when an upload to object storage was attempted but failed."""


class SkillArchiveFetchError(SkillArchiveStorageError):
    """Raised when fetching a skill archive from object storage failed."""


def is_configured() -> bool:
    """True iff object storage is usable; False means callers store inline."""
    return is_storage_enabled()


def build_storage_key(user_id: str, content_hash: str) -> str:
    """Build the content-addressed ``user-skills/{user_id}/{hash}.zip`` key."""
    if not _USER_ID_RE.match(user_id):
        msg = f"Refusing to build storage key for unsafe user_id: {user_id!r}"
        raise SkillArchiveStorageError(msg)
    if not _CONTENT_HASH_RE.match(content_hash):
        msg = f"Refusing to build storage key for unsafe content_hash: {content_hash!r}"
        raise SkillArchiveStorageError(msg)
    return f"user-skills/{user_id}/{content_hash.removeprefix('sha256:')}.zip"


async def store_archive(*, user_id: str, content: bytes, content_hash: str) -> str | None:
    """Upload canonical archive bytes; return the storage key.

    Returns ``None`` when storage is not configured (caller stores inline).
    Raises :class:`SkillArchiveUploadError` when configured but the upload failed.
    """
    if not is_configured():
        return None

    storage_key = build_storage_key(user_id, content_hash)
    # upload_bytes is a synchronous boto3 call; off-load so we don't block the
    # event loop (same pattern as memo_binary_storage).
    success = await asyncio.to_thread(
        _storage_upload_bytes, storage_key, content, _ARCHIVE_CONTENT_TYPE
    )
    if not success:
        logger.error(
            "Failed to upload skill archive to object storage (user=%s key=%s)",
            user_id,
            storage_key,
        )
        msg = "Could not store the skill archive. Please retry."
        raise SkillArchiveUploadError(msg)
    return storage_key


async def delete_archive(storage_key: str | None) -> bool:
    """Best-effort delete. Never raises — an orphan object is a hygiene issue,
    not a correctness one, once the row referencing it is gone."""
    if not storage_key or not isinstance(storage_key, str):
        return False
    if not is_configured():
        return False
    try:
        return bool(await asyncio.to_thread(_storage_delete_object, storage_key))
    except Exception:
        logger.exception(
            "skill archive delete failed (orphan left behind)",
            extra={"storage_key": storage_key},
        )
        return False


async def fetch_archive(storage_key: str) -> bytes:
    """Fetch archive bytes by key.

    Raises:
        SkillArchiveFetchError: malformed key, storage no longer configured,
            or the object could not be downloaded.
    """
    if not storage_key or not isinstance(storage_key, str):
        msg = "storage_key must be a non-empty string"
        raise SkillArchiveFetchError(msg)
    if not is_configured():
        msg = f"Object storage is not configured; cannot fetch skill archive {storage_key}"
        raise SkillArchiveFetchError(msg)

    data = await asyncio.to_thread(_storage_get_bytes, storage_key)
    if data is None:
        msg = f"Skill archive not found or unreadable at {storage_key}"
        raise SkillArchiveFetchError(msg)
    return data
