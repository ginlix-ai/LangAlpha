"""Key format, size cap and registry SQL for workspace file blobs. Import-free by design.

Split out of :mod:`workspace_file_blobs` so the operator scripts under
``scripts/ops/`` can share these definitions rather than keep a second copy in
sync by hand. That module imports ``database.pool`` -> ``src.config.env`` ->
``load_dotenv()``, which would silently retarget a mutating script at whatever
``.env`` happens to be on disk. Nothing here imports anything but ``re``, so
both sides can agree on one definition.
"""

from __future__ import annotations

import re

BLOB_KEY_PREFIX = "blobs/"

# One blob backs files with different names and MIME types, so any per-file
# content type would be write-order-dependent and wrong for some referrer.
# Serving derives Content-Type from the file's extension instead.
BLOB_CONTENT_TYPE = "application/octet-stream"

# Per-object cap, passed explicitly to the storage facade so its shared
# STORAGE_MAX_UPLOAD_SIZE default (10MB, sized for avatars and charts) can't
# reject a file the sync path already accepted. ``FilePersistenceService``
# derives MAX_FILE_SIZE from this: a file the sync path accepts must be storable.
MAX_BLOB_BYTES = 100 * 1024 * 1024

# Garbage-collection windows, shared with scripts/ops/report_orphan_blobs.py.
# A blob is condemned after GC_GRACE_DAYS with no reference and no writer
# touch, and its object is deleted GC_CONDEMNED_GRACE_HOURS after that. Any
# sync that saw the row as live before condemnation has long since committed
# or failed by the time the object goes.
GC_GRACE_DAYS = 7
GC_CONDEMNED_GRACE_HOURS = 24

# A real SHA-256 hexdigest. The object key is derived from the digest, so
# anything that isn't a clean digest must never reach the store as a key.
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")

# A user id as the store sees it: one path segment, no separators, no dots
# leading it. ``workspaces.user_id`` is free text up to 255 characters, so
# the shape is checked here rather than assumed.
_USER_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._@+-]{0,254}\Z")


class BlobError(Exception):
    """Base class for workspace file blob failures."""


def blob_key(user_id: str, sha256: str) -> str:
    """Return the object key for one user's copy of a digest.

    Keys are scoped per user so a presigned URL, and any sandbox holding
    one, can only ever name objects under its own user; identical bytes
    held by two users are two objects. Both segments are validated, since
    the key is built from them and nothing else.
    """
    if not user_id or not _USER_ID_RE.match(user_id):
        msg = f"Refusing to build a blob key for a malformed user id: {user_id!r}"
        raise BlobError(msg)
    if not sha256 or not _SHA256_RE.match(sha256):
        msg = f"Refusing to build a blob key for a non-digest: {sha256!r}"
        raise BlobError(msg)
    return f"{BLOB_KEY_PREFIX}{user_id}/{sha256}"


# --- registry SQL -------------------------------------------------------------
#
# The statements a writer and the sweep have to agree on, character for
# character. The backfill script runs the same protocol as the app; a second
# hand-copy of any of these is how the two silently diverge.

# Registry upsert shared by every writer. Reviving a condemned row is the
# writer's half of the protocol: its upload precedes this statement, so a
# revived row always has an object behind it again.
REGISTER_SQL = """
    INSERT INTO workspace_file_blobs (user_id, sha256, byte_len)
    VALUES (%s, %s, %s)
    ON CONFLICT (user_id, sha256) DO UPDATE
        SET condemned_at = NULL,
            last_referenced_at = NOW()
"""

# A writer's claim on a condemned row, ordered before its upload. Restamping
# condemned_at takes the row out of the reap's grace predicate for another
# GC_CONDEMNED_GRACE_HOURS; if the reap already holds the row lock this
# blocks, then matches nothing, and the writer inserts fresh after uploading.
CLAIM_SQL = """
    UPDATE workspace_file_blobs
       SET condemned_at = NOW()
     WHERE user_id = %s AND sha256 = ANY(%s) AND condemned_at IS NOT NULL
"""

# Every column a manifest row can reference a blob through: its own object, or
# the pack chunk it is a member of. Written against a ``workspace_file_blobs
# b``; the condemn pass, the reap re-check and the orphan report must agree on
# it exactly. A row references a registry entry only through a workspace of
# the same user, since that user's object is the one the row's bytes are in.
# Two EXISTS rather than one OR so each side can use its partial index.
REFERENCED_SQL = (
    "(EXISTS (SELECT 1 FROM workspace_files f"
    " JOIN workspaces w ON w.workspace_id = f.workspace_id"
    " WHERE f.blob_sha256 = b.sha256 AND w.user_id = b.user_id)"
    " OR EXISTS (SELECT 1 FROM workspace_files f"
    " JOIN workspaces w ON w.workspace_id = f.workspace_id"
    " WHERE f.pack_sha256 = b.sha256 AND w.user_id = b.user_id))"
)

# The manifest side of the same relation: an unaliased ``workspace_files``
# predicate for a row whose bytes live in object storage. It enumerates the
# same two pointer columns as REFERENCED_SQL, so a third one has to land in
# both.
BLOB_BACKED_SQL = "(blob_sha256 IS NOT NULL OR pack_sha256 IS NOT NULL)"
