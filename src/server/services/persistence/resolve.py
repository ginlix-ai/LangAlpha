"""The single byte-resolution ladder shared by every read path.

Inline columns, then the blob a row names, then the pack member it points
into. Restore and every serving route come through here.
"""

import logging

from src.server.database.workspace_file_blobs import (
    BlobError,
    fetch_blob,
    fetch_blob_range,
)

logger = logging.getLogger(__name__)


class FileBytesUnavailable(Exception):
    """A file's bytes live in object storage and could not be read right now.

    Distinct from "no such file": the manifest row exists and names a blob, so
    the correct answer is a transient failure, not a 404.
    """


async def resolve_file_bytes(file_record: dict, *, user_id: str) -> bytes | None:
    """Return a file record's raw bytes, from inline columns or its blob.

    The single byte-resolution ladder for every read path. Inline columns are
    checked with ``is not None`` rather than truthiness: a row holding ``b""``
    or ``""`` is populated, and truthiness would turn an empty file into a 404
    (or, for a blob-backed row, into a pointless network fetch). Returns
    ``None`` only when the file genuinely has no content anywhere; raises
    :class:`FileBytesUnavailable` when a blob it names can't be read.

    ``user_id`` is the workspace owner's, which names the object-storage
    namespace the row's digest lives in; the row itself carries no user.
    """
    content_binary = file_record.get("content_binary")
    if content_binary is not None:
        return bytes(content_binary) if isinstance(content_binary, memoryview) else content_binary

    content_text = file_record.get("content_text")
    if content_text is not None:
        return content_text.encode("utf-8")

    blob_sha256 = file_record.get("blob_sha256")
    if blob_sha256:
        try:
            return await fetch_blob(user_id, blob_sha256)
        except BlobError as e:
            raise FileBytesUnavailable(str(e)) from e

    pack_sha256 = file_record.get("pack_sha256")
    if pack_sha256:
        try:
            return await fetch_blob_range(
                user_id,
                pack_sha256,
                int(file_record.get("pack_offset") or 0),
                int(file_record.get("file_size") or 0),
                expected_sha256=file_record.get("content_hash"),
            )
        except BlobError as e:
            raise FileBytesUnavailable(str(e)) from e

    return None


async def resolve_file_bytes_or_none(
    file_record: dict, *, user_id: str, context: str
) -> bytes | None:
    """Resolve bytes for a route whose only credential is the URL itself.

    An unfetchable blob collapses to the same ``None`` as an absent file. These
    routes (``wsfiles``, the share-token endpoints) authenticate nothing beyond
    an opaque id in the path, so answering 503 for "storage is down" and 404
    for "no such file" would confirm to a guesser that a workspace and path
    exist. The uniform answer is a security property — it lives here, in one
    named policy, rather than as a rule each route is trusted to remember.
    """
    try:
        return await resolve_file_bytes(file_record, user_id=user_id)
    except FileBytesUnavailable as e:
        logger.warning(f"Blob unavailable {context}: {e}")
        return None


async def resolve_file_text_or_none(
    file_record: dict, *, user_id: str, context: str
) -> str | None:
    """:func:`resolve_file_bytes_or_none`, decoded. Same uniform-absent policy.

    ``replace`` is a guard, not a decoding strategy: the write path only leaves
    ``is_binary`` false after a strict UTF-8 decode of the whole file, so a
    caller that honours the flag never reaches a replacement character. One
    that doesn't gets mojibake instead of an exception.
    """
    data = await resolve_file_bytes_or_none(
        file_record, user_id=user_id, context=context
    )
    if data is None:
        return None
    return data.decode("utf-8", errors="replace")
