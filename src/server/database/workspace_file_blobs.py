"""Content-addressed store for workspace file bytes.

A workspace file keeps its manifest row in ``workspace_files`` and its bytes
in object storage under ``blobs/{user_id}/{sha256}``; ``workspace_file_blobs``
is the registry of ``(user_id, sha256)`` pairs that have been written. Content
addressing dedups identical files across a user's workspaces, and lets a
forked workspace share bytes rather than copy TOASTed BYTEA. The user is part
of the key, so a presigned URL can only name that user's objects and a
registry hit is always a pointer into bytes the same user uploaded.

The key format, the size cap and the registry statements live in
:mod:`src.server.database.blob_keys`, which is import-free so ``scripts/ops/``
can run the same protocol.

Deletion is the one irrecoverable operation here, and the reference to a
blob lands seconds after its upload in a separate transaction, so a sweep
cannot simply delete what nothing references. The registry carries a
protocol instead. Writers touch ``last_referenced_at`` before they upload
(``registered_blobs``); a row nothing references and nothing has touched for
``GC_GRACE_DAYS`` is condemned (``condemn_orphan_blobs``), which makes it
invisible to that touch. A writer that still wants the digest claims the
condemned row by restamping ``condemned_at``, uploads, and only then revives
it (``register_blobs`` / ``store_blob``), so a live row always has an object.
``GC_CONDEMNED_GRACE_HOURS`` after condemnation the object is deleted under
the row's lock (``reap_condemned_blobs``), and the lock is taken with the
grace predicate: a claim that lands first pushes the row out of the
predicate, and a claim that lands second queues behind the lock and finds no
row, so the writer inserts a fresh one after its upload. No manifest row can
point at a deleted object.

The residual the ordering accepts: an upload that lands and is then abandoned
before its row is written, because the process was cancelled or the register
failed, leaves an object no sweep can see, since every pass enumerates the
registry rather than the bucket. The next sync of the same content re-uploads
the same key and registers it, so it is reclaimable for as long as the content
survives; a digest that never recurs is not. That is the price of the
direction, and the direction is the safe one, since the alternative deletes
objects a concurrent writer may already have committed a manifest row against.

DEPLOYMENT REQUIREMENT — the ``blobs/`` prefix must not be anonymously
readable. The bucket this facade writes to may also be fronted by a public URL
base (``STORAGE_PUBLIC_URL_BASE``) for avatars and chart images. Where that
base serves arbitrary keys, a content-addressed key is guessable for any file
whose bytes an attacker can reconstruct, so the digest is obscurity rather
than access control. Deny ``blobs/`` on the public base, or point blob storage
at a bucket that has none, before enabling this in a hosted deployment.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging

from src.server.database.blob_keys import (
    BLOB_CONTENT_TYPE,
    CLAIM_SQL,
    GC_CONDEMNED_GRACE_HOURS,
    GC_GRACE_DAYS,
    MAX_BLOB_BYTES,
    REFERENCED_SQL,
    REGISTER_SQL,
    BlobError,
    blob_key,
)
from src.server.database.pool import get_db_connection
from src.server.database.session_lock import await_settled, release_session_lock
from src.utils.storage import (
    delete_object as _storage_delete_object,
    get_bytes as _storage_get_bytes,
    get_bytes_range as _storage_get_bytes_range,
    upload_bytes as _storage_upload_bytes,
)

logger = logging.getLogger(__name__)

__all__ = [
    "BlobError",
    "BlobFetchError",
    "BlobUploadError",
    "GC_CONDEMNED_GRACE_HOURS",
    "GC_GRACE_DAYS",
    "GC_REAP_BATCH",
    "blob_key",
    "condemn_orphan_blobs",
    "fetch_blob",
    "fetch_blob_range",
    "reap_condemned_blobs",
    "register_blobs",
    "registered_blobs",
    "store_blob",
    "sweep_blob_garbage",
]

# Objects deleted per sweep cycle. Bounds the store calls a cycle makes; the
# report script shows the backlog if churn ever outruns it.
GC_REAP_BATCH = 500

_GC_LOCK_KEY = "workspace_file_blobs:gc"

# The writer's touch: restamps last_referenced_at so the sweep leaves the row
# alone until the manifest row referencing it has landed, and restamps
# condemned_at on a condemned row (the claim) so the caller's re-upload is
# ordered before any reap of it.
_TOUCH_SQL = """
    UPDATE workspace_file_blobs
       SET last_referenced_at = NOW(),
           condemned_at = CASE WHEN condemned_at IS NULL THEN NULL ELSE NOW() END
     WHERE user_id = %s AND sha256 = ANY(%s)
    RETURNING sha256, condemned_at IS NULL AS live
"""


class BlobUploadError(BlobError):
    """Raised when a blob could not be written to object storage."""


class BlobFetchError(BlobError):
    """Raised when a blob's bytes could not be read back."""


async def store_blob(user_id: str, sha256: str, data: bytes) -> None:
    """Write ``data`` to object storage under the user's digest key and register it.

    The object lands before the registry row, so a registered blob always has
    bytes behind it. This is the relay path's writer; callers skip it for
    digests ``registered_blobs`` already reports, since a registry row is
    proof the object exists with verified content. The claim comes first so
    a concurrent reap cannot delete the object between the upload and the
    row.
    """
    key = blob_key(user_id, sha256)
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(CLAIM_SQL, (user_id, [sha256]))
    except Exception as e:
        msg = f"Blob claim failed for {key}"
        raise BlobUploadError(msg) from e
    uploaded = await asyncio.to_thread(
        _storage_upload_bytes,
        key,
        data,
        BLOB_CONTENT_TYPE,
        max_size=MAX_BLOB_BYTES,
    )
    if not uploaded:
        msg = f"Blob upload failed for {key} ({len(data)} bytes)"
        raise BlobUploadError(msg)

    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(REGISTER_SQL, (user_id, sha256, len(data)))
    except Exception as e:
        # The object is already durable; leaving it behind is correct. It is a
        # content-addressed key another of this user's syncs may have committed
        # a manifest row against, so deleting it here could strand that row.
        msg = f"Blob registry insert failed for {key}"
        raise BlobUploadError(msg) from e


async def registered_blobs(user_id: str, sha256s: list[str]) -> set[str]:
    """Which of these digests the user may skip uploading.

    A live registry row under the user is proof the bytes are there: it is
    written only after a checksum-bound upload to that user's key succeeded,
    or after this process hashed the bytes itself. Another user's row for the
    same digest is a different object and never a hit, so a caller that
    merely names a digest is told nothing about who else holds the content.

    The check is also the touch that keeps the sweep off the row: a digest
    named here cannot be condemned for another ``GC_GRACE_DAYS``, long enough
    for the manifest row that references it to land. A condemned row is never
    a hit but is claimed in the same statement, so the caller's upload is
    ordered before any reap of it; ``register_blobs`` then revives it.
    """
    if not sha256s:
        return set()
    digests = list(set(sha256s))
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_TOUCH_SQL, (user_id, digests))
            return {row[0] for row in await cur.fetchall() if row[1]}


async def register_blobs(user_id: str, entries: list[tuple[str, int]]) -> None:
    """Record ``(sha256, byte_len)`` pairs whose objects are known to exist
    under the user, reviving any the sweep had condemned in the meantime."""
    if not entries:
        return
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                REGISTER_SQL, [(user_id, sha, n) for sha, n in entries]
            )


async def fetch_blob(user_id: str, sha256: str) -> bytes:
    """Read a blob's bytes back, verifying they still hash to their key.

    The digest check is free relative to the download and turns silent
    corruption (or a mis-keyed object) into a loud error rather than a file
    that restores wrong.
    """
    key = blob_key(user_id, sha256)
    data = await asyncio.to_thread(_storage_get_bytes, key)
    # `is None` deliberately: sha256(b"") is a valid blob and b"" is falsy.
    if data is None:
        msg = f"Blob {key} could not be read from object storage"
        raise BlobFetchError(msg)
    actual = hashlib.sha256(data).hexdigest()
    if actual != sha256:
        msg = f"Blob {key} content hash mismatch (got {actual})"
        raise BlobFetchError(msg)
    return data


async def fetch_blob_range(
    user_id: str,
    sha256: str,
    offset: int,
    length: int,
    *,
    expected_sha256: str | None = None,
) -> bytes:
    """Read one member's bytes out of a pack chunk, verifying them.

    A member row carries the file's own digest as ``content_hash``; checking
    the slice against it catches a wrong offset or a mis-keyed chunk as
    loudly as :func:`fetch_blob` catches a corrupt object.
    """
    key = blob_key(user_id, sha256)
    data = await asyncio.to_thread(_storage_get_bytes_range, key, offset, length)
    if data is None:
        msg = f"Pack {key} could not be read from object storage"
        raise BlobFetchError(msg)
    if len(data) != length:
        msg = f"Pack {key} range {offset}+{length} returned {len(data)} bytes"
        raise BlobFetchError(msg)
    if expected_sha256 is not None:
        actual = hashlib.sha256(data).hexdigest()
        if actual != expected_sha256:
            msg = f"Pack {key} member at {offset} hash mismatch (got {actual})"
            raise BlobFetchError(msg)
    return data


# --- garbage collection ------------------------------------------------------


async def condemn_orphan_blobs(grace_days: int = GC_GRACE_DAYS, *, conn=None) -> int:
    """Pass 1: mark rows nothing references and nothing has touched in ``grace_days``.

    A concurrent touch and this UPDATE serialize on the row: whichever runs
    second re-evaluates its WHERE against the other's result, so a touched
    row is never condemned and a condemned row is never returned as live.
    """
    async with get_db_connection(conn) as c:
        async with c.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE workspace_file_blobs b
                   SET condemned_at = NOW()
                 WHERE b.condemned_at IS NULL
                   AND b.last_referenced_at < NOW() - make_interval(days => %s)
                   AND NOT {REFERENCED_SQL}
                """,
                (grace_days,),
            )
            return cur.rowcount


async def _reap_one(
    conn, user_id: str, sha256: str, condemned_grace_hours: int
) -> bool:
    """Delete one condemned blob's object and row in one transaction.

    ``FOR UPDATE`` with the grace predicate is what closes the race with a
    writer. A claim that landed first restamped ``condemned_at`` and the row
    no longer matches; a claim that lands later blocks behind the lock, then
    matches nothing, and the writer uploads before inserting a fresh row. A
    store failure raises, which rolls the transaction back and leaves the row
    condemned for the next cycle.
    """
    async with conn.transaction():
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT 1 FROM workspace_file_blobs
                 WHERE user_id = %s AND sha256 = %s
                   AND condemned_at < NOW() - make_interval(hours => %s)
                   FOR UPDATE
                """,
                (user_id, sha256, condemned_grace_hours),
            )
            if await cur.fetchone() is None:
                return False  # revived, or reaped by another cycle
            await cur.execute(
                "SELECT 1 FROM workspace_file_blobs b "
                f"WHERE b.user_id = %s AND b.sha256 = %s AND {REFERENCED_SQL}",
                (user_id, sha256),
            )
            if await cur.fetchone() is not None:
                # A reference appeared after condemnation. Fork copies only
                # referenced rows so this should not happen; heal rather than
                # delete an object a manifest row points at.
                await cur.execute(
                    "UPDATE workspace_file_blobs SET condemned_at = NULL, "
                    "last_referenced_at = NOW() WHERE user_id = %s AND sha256 = %s",
                    (user_id, sha256),
                )
                return False
            key = blob_key(user_id, sha256)
            # Awaited to completion however often the sweep is cancelled at
            # shutdown. The thread cannot be interrupted, and this
            # transaction's row lock is the only thing stopping a writer from
            # claiming, uploading and reviving the digest while it runs: a
            # rollback would release the lock and the thread would then
            # delete that writer's fresh object.
            deleted = await await_settled(
                asyncio.ensure_future(asyncio.to_thread(_storage_delete_object, key))
            )
            if not deleted:
                msg = f"Object delete failed for {key}"
                raise BlobError(msg)
            await cur.execute(
                "DELETE FROM workspace_file_blobs WHERE user_id = %s AND sha256 = %s",
                (user_id, sha256),
            )
            return True


async def reap_condemned_blobs(
    condemned_grace_hours: int = GC_CONDEMNED_GRACE_HOURS,
    limit: int = GC_REAP_BATCH,
    *,
    conn=None,
) -> tuple[int, int]:
    """Pass 2: delete objects condemned longer than the grace, oldest first.

    Returns ``(deleted, failed)``. One transaction per row so a single store
    failure costs one row, not the batch.
    """
    async with get_db_connection(conn) as c:
        async with c.cursor() as cur:
            await cur.execute(
                """
                SELECT user_id, sha256 FROM workspace_file_blobs
                 WHERE condemned_at < NOW() - make_interval(hours => %s)
                 ORDER BY condemned_at
                 LIMIT %s
                """,
                (condemned_grace_hours, limit),
            )
            candidates = [(row[0], row[1]) for row in await cur.fetchall()]
        deleted = failed = 0
        for user_id, sha256 in candidates:
            try:
                if await _reap_one(c, user_id, sha256, condemned_grace_hours):
                    deleted += 1
            except Exception as e:
                failed += 1
                logger.warning(
                    f"Blob reap of {sha256} for user {user_id} failed, will retry: {e}"
                )
        return deleted, failed


async def sweep_blob_garbage(
    *,
    grace_days: int = GC_GRACE_DAYS,
    condemned_grace_hours: int = GC_CONDEMNED_GRACE_HOURS,
    limit: int = GC_REAP_BATCH,
) -> dict[str, int] | None:
    """One condemn-then-reap cycle under a session advisory lock.

    Returns the cycle's counts, or ``None`` when another process holds the
    lock. The lock is session-scoped rather than transaction-scoped because
    the reap runs one transaction per row; it is released before the
    connection goes back to the pool.
    """
    async with get_db_connection() as conn:
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                    (_GC_LOCK_KEY,),
                )
                row = await cur.fetchone()
        except BaseException:
            # The grant may already have landed: a session lock survives a
            # cancelled fetch, so release it rather than pool a locked session.
            await release_session_lock(conn, _GC_LOCK_KEY)
            raise
        if not (row and row[0]):
            return None
        try:
            condemned = await condemn_orphan_blobs(grace_days, conn=conn)
            deleted, failed = await reap_condemned_blobs(
                condemned_grace_hours, limit, conn=conn
            )
        finally:
            await release_session_lock(conn, _GC_LOCK_KEY)
    return {"condemned": condemned, "deleted": deleted, "failed": failed}
