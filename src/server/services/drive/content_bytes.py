"""A Drive file's bytes: how they are told apart and named, where they are
kept and read back, and the room a save's body takes while it is held.

Postgres holds text, and binary when object storage is off; other bytes go
to object storage under the space owner's blob keys, stored before the
transaction that records them.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

from src.server.database.blob_keys import BlobError
from src.server.database.drive import reads
from src.server.database.drive.records import Content, DriveBusy, DriveError
from src.server.database.workspace_file_blobs import (
    fetch_blob_range,
    registered_blobs,
    store_blob,
)
from src.server.services.drive.data_files import is_data_file
from src.server.services.persistence.transfer import held_bytes_budget
from src.utils.storage import is_storage_enabled

logger = logging.getLogger(__name__)

#: Text is held in Postgres up to this size, whatever the storage.
TEXT_MAX_BYTES = 1024 * 1024
#: How long a save waits for room among the bytes this worker already holds.
SAVE_WAIT_S = 5.0

_DB_CHUNK = 1024 * 1024
_STORE_CHUNK = 4 * 1024 * 1024
#: Bodies above this are hashed in a thread, off the event loop.
_HASH_INLINE_MAX = 1024 * 1024


def is_text(data: bytes) -> bool:
    if len(data) > TEXT_MAX_BYTES or b"\x00" in data:
        return False
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


def kept_as_text(name: str, data: bytes) -> bool:
    """Whether the Drive keeps a file named ``name`` as text: by its bytes,
    except that with object storage a data file never is."""
    if is_storage_enabled() and is_data_file(name):
        return False
    return is_text(data)


def content_type(name: str, text: bool, given: str | None) -> str:
    if given:
        return given[:255]
    guessed, _ = mimetypes.guess_type(name, strict=False)
    if guessed:
        return guessed
    return "text/plain" if text else "application/octet-stream"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


async def digest(data: bytes) -> str:
    if len(data) <= _HASH_INLINE_MAX:
        return _sha256(data)
    return await asyncio.to_thread(_sha256, data)


@asynccontextmanager
async def holding(size: int) -> AsyncIterator[None]:
    """Room for a save's ``size`` bytes among the whole files this worker
    holds (``held_bytes_budget``), taken before the body is read. Waits up
    to ``SAVE_WAIT_S`` and then raises ``DriveBusy``, which a surface
    sends as 503 with ``Retry-After``."""
    held = AsyncExitStack()
    try:
        async with asyncio.timeout(SAVE_WAIT_S):
            await held.enter_async_context(held_bytes_budget().hold(size))
    except TimeoutError:
        raise DriveBusy() from None
    async with held:
        yield


def _unstored(sha: str, e: BlobError) -> DriveError:
    logger.warning("[Drive] store of %s failed: %s", sha, e)
    return DriveError("unavailable", "The file could not be stored now")


async def registered(user_id: str, sha: str) -> bool:
    """Whether ``user_id``'s object storage already holds ``sha``."""
    try:
        return sha in await registered_blobs(user_id, [sha])
    except BlobError as e:
        raise _unstored(sha, e) from e


async def upload(user_id: str, sha: str, data: bytes) -> None:
    try:
        await store_blob(user_id, sha, data)
    except BlobError as e:
        raise _unstored(sha, e) from e


async def read_range(found: Content, offset: int, length: int) -> bytes:
    offset = max(0, min(offset, found.size))
    length = max(0, min(length, found.size - offset))
    if length == 0:
        return b""
    if found.blob_user_id is None:
        return await reads.held_bytes(found.space_id, found.sha256, offset, length)
    whole = offset == 0 and length == found.size
    try:
        return await fetch_blob_range(
            found.blob_user_id,
            found.sha256,
            offset,
            length,
            expected_sha256=found.sha256 if whole else None,
        )
    except BlobError as e:
        logger.warning("[Drive] read of %s failed: %s", found.sha256, e)
        raise DriveError("unavailable", "The file's bytes cannot be read now") from e


async def stream(found: Content, start: int = 0, end: int | None = None) -> AsyncIterator[bytes]:
    """Bytes ``start`` to ``end`` (exclusive), a chunk at a time."""
    end = found.size if end is None else min(end, found.size)
    chunk = _DB_CHUNK if found.blob_user_id is None else _STORE_CHUNK
    at = start
    while at < end:
        n = min(chunk, end - at)
        yield await read_range(found, at, n)
        at += n
