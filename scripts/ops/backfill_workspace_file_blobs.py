"""Move existing inline workspace file bytes into content-addressed object storage.

Migration 039 adds ``workspace_files.blob_sha256`` but backfills nothing: rows
written before it keep their bytes in ``content_text`` / ``content_binary``.
This script uploads those bytes to ``blobs/{user_id}/{sha256}`` under the
workspace owner, registers the digest, and NULLs the inline columns: the
actual reduction in database size, backup volume, and replica traffic that
motivated the change.

Run it AFTER the new code is deployed and verified, never before: a row whose
bytes have moved is unreadable to code that doesn't know about ``blob_sha256``.
``--reverse`` pulls the bytes back inline and clears the pointer, which is the
rollback path — NULLing the columns is otherwise a one-way door.

Usage:
    uv run python scripts/ops/backfill_workspace_file_blobs.py            # dry-run
    uv run python scripts/ops/backfill_workspace_file_blobs.py --workspace UUID --apply
    uv run python scripts/ops/backfill_workspace_file_blobs.py --apply
    uv run python scripts/ops/backfill_workspace_file_blobs.py --reverse --apply

Rows whose owner has a non-UUID id are left inline unless
``--include-legacy-owners`` is passed: such an account is re-keyed by its first
platform login, and objects keyed by the old id would not follow it.

Idempotent in both directions. Connects via the same DB_* env vars the app
uses (``src/server/database/pool.py``).
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

# Run as a script, sys.path[0] is scripts/ops — not the repo root the `src`
# package lives under. Same prelude as scripts/utils/render_prompt.py.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Every one of these is import-clean: _db reads os.environ, blob_keys imports
# only `re`, and src.utils.storage only os + yaml + the provider SDK.
# Deliberately NOT src.server.database.workspace_file_blobs, which pulls
# database/pool -> src.config.env -> load_dotenv() and would silently retarget
# a mutating operator script at whatever .env happens to be on disk.
from scripts.ops._db import build_db_uri
from src.server.database.blob_keys import (
    BLOB_BACKED_SQL,
    BLOB_CONTENT_TYPE,
    CLAIM_SQL,
    MAX_BLOB_BYTES,
    REGISTER_SQL,
    blob_key,
)
from src.utils.storage import (
    get_bytes,
    get_bytes_range,
    is_storage_enabled,
    upload_bytes,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_workspace_file_blobs")

# Uploads in flight per chunk, and the byte ceiling that overrides the count so
# a chunk of large files can't hold UPLOAD_CONCURRENCY x MAX_BLOB_BYTES at once.
UPLOAD_CONCURRENCY = 8
INFLIGHT_MAX_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class _Pending:
    """One row whose bytes are in hand, waiting to be uploaded and repointed."""

    file_id: str
    user_id: str
    sha256: str
    data: bytes
    stored_hash: str | None
    workspace_id: str
    file_path: str


@dataclass
class _Counts:
    moved: int = 0
    moved_bytes: int = 0
    # Stored content_hash disagrees with the inline bytes: the inline copy is
    # lossy and must not be canonized. Distinct from `raced`.
    skipped_lossy: int = 0
    skipped_empty: int = 0
    # The owner's id is not the UUID a platform login presents, so a legacy
    # login could still re-key the account and leave its objects behind.
    skipped_legacy_owner: int = 0
    # The row changed between reading its bytes and writing the pointer.
    skipped_raced: int = 0
    failed: int = 0
    pending: list[_Pending] = field(default_factory=list)
    pending_bytes: int = 0


def _inline_bytes(row: dict) -> bytes | None:
    """Return a row's inline bytes, preferring the lossless BYTEA column."""
    content_binary = row.get("content_binary")
    if content_binary is not None:
        return bytes(content_binary)
    content_text = row.get("content_text")
    if content_text is not None:
        return content_text.encode("utf-8")
    return None


async def _next_ids(
    conn: psycopg.AsyncConnection,
    where: str,
    after: str | None,
    limit: int,
    workspace_id: str | None,
) -> list[str]:
    """Fetch the next page of matching ids, keyset-paginated on the PK.

    Ids only. A 500-row SELECT that includes the content columns detoasts up to
    500 x 100MB into memory at once; this reads only the null bitmap.
    """
    params: list = []
    scope_clause = ""
    if workspace_id is not None:
        scope_clause = "AND workspace_id = %s::uuid"
        params.append(workspace_id)
    cursor_clause = ""
    if after is not None:
        cursor_clause = "AND workspace_file_id > %s::uuid"
        params.append(after)
    params.append(limit)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT workspace_file_id
            FROM workspace_files
            WHERE {where} {scope_clause} {cursor_clause}
            ORDER BY workspace_file_id
            LIMIT %s
            """,
            params,
        )
        return [str(r["workspace_file_id"]) for r in await cur.fetchall()]


async def _load_row(conn: psycopg.AsyncConnection, file_id: str) -> dict | None:
    """One row with its bytes and its owner, who names the storage namespace."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT f.workspace_file_id, f.workspace_id, w.user_id, f.file_path,
                   f.content_hash, f.content_text, f.content_binary,
                   f.blob_sha256, f.pack_sha256, f.pack_offset, f.file_size,
                   f.is_binary
            FROM workspace_files f
            JOIN workspaces w ON w.workspace_id = f.workspace_id
            WHERE f.workspace_file_id = %s::uuid
            """,
            (file_id,),
        )
        return await cur.fetchone()


async def _upload_chunk(pending: list[_Pending]) -> list[bool | BaseException]:
    """Upload a chunk concurrently. A serial round trip per row measures ~3
    rows/s, which is untenable for a large table."""
    sem = asyncio.Semaphore(UPLOAD_CONCURRENCY)

    async def _one(item: _Pending) -> bool:
        async with sem:
            return await asyncio.to_thread(
                upload_bytes,
                blob_key(item.user_id, item.sha256),
                item.data,
                BLOB_CONTENT_TYPE,
                max_size=MAX_BLOB_BYTES,
            )

    return await asyncio.gather(
        *(_one(item) for item in pending), return_exceptions=True
    )


async def _repoint_row(conn: psycopg.AsyncConnection, item: _Pending) -> bool:
    """Register the digest and move the row onto it. Returns False if it raced.

    One transaction: the sweep only spares objects with a registry row, so a
    crash between the two must not leave a row pointing at an unregistered
    digest.
    """
    async with conn.transaction(), conn.cursor() as cur:
        await cur.execute(REGISTER_SQL, (item.user_id, item.sha256, len(item.data)))
        # The content_hash predicate makes this safe against a concurrent
        # re-sync that rewrote the row while we held its bytes: a changed row
        # doesn't match and stays inline.
        await cur.execute(
            """
            UPDATE workspace_files
               SET blob_sha256 = %s,
                   content_text = NULL,
                   content_binary = NULL,
                   file_size = %s
             WHERE workspace_file_id = %s::uuid
               AND blob_sha256 IS NULL
               AND pack_sha256 IS NULL
               AND content_hash IS NOT DISTINCT FROM %s
            """,
            (item.sha256, len(item.data), item.file_id, item.stored_hash),
        )
        return bool(cur.rowcount)


async def _flush(conn: psycopg.AsyncConnection, counts: _Counts) -> None:
    """Upload the pending chunk concurrently, then apply its DB writes serially
    (psycopg connections are not safe to share across concurrent tasks)."""
    if not counts.pending:
        return
    # Claim any digest the sweeper has condemned before uploading it, so a
    # concurrent reap cannot delete the object between the PUT and the row.
    # The registry is keyed per user, so the claim is one statement per owner.
    by_user: dict[str, list[str]] = {}
    for item in counts.pending:
        by_user.setdefault(item.user_id, []).append(item.sha256)
    async with conn.cursor() as cur:
        for user_id, digests in by_user.items():
            await cur.execute(CLAIM_SQL, (user_id, digests))
    uploads = await _upload_chunk(counts.pending)

    for item, ok in zip(counts.pending, uploads, strict=True):
        if ok is not True:
            counts.failed += 1
            logger.error(
                "upload failed for %s (%d bytes): %s",
                blob_key(item.user_id, item.sha256),
                len(item.data),
                ok if isinstance(ok, BaseException) else "rejected",
            )
            continue
        if await _repoint_row(conn, item):
            counts.moved += 1
            counts.moved_bytes += len(item.data)
        else:
            counts.skipped_raced += 1
            logger.warning(
                "row changed under us, left inline: %s %s",
                item.workspace_id,
                item.file_path,
            )

    counts.pending = []
    counts.pending_bytes = 0


def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(str(value))
    except ValueError:
        return False
    return True


async def _forward(
    conn: psycopg.AsyncConnection,
    apply: bool,
    batch: int,
    workspace_id: str | None,
    include_legacy_owners: bool = False,
) -> int:
    where = (
        "blob_sha256 IS NULL "
        "AND (content_text IS NOT NULL OR content_binary IS NOT NULL)"
    )
    after: str | None = None
    counts = _Counts()
    legacy_owners: set[str] = set()

    while True:
        ids = await _next_ids(conn, where, after, batch, workspace_id)
        if not ids:
            break
        after = ids[-1]

        for file_id in ids:
            row = await _load_row(conn, file_id)
            if row is None or row["blob_sha256"] is not None:
                continue
            if not include_legacy_owners and not _is_uuid(row["user_id"]):
                # Objects are keyed by owner id. An account whose id predates
                # platform auth is re-keyed by its first login, and nothing
                # moves its objects with it; leave its bytes inline until the
                # operator confirms no such login can still arrive.
                if row["user_id"] not in legacy_owners:
                    legacy_owners.add(row["user_id"])
                    logger.warning(
                        "owner %s has a non-UUID id; leaving its rows inline "
                        "(--include-legacy-owners to move them)",
                        row["user_id"],
                    )
                counts.skipped_legacy_owner += 1
                continue
            data = _inline_bytes(row)
            if data is None:
                counts.skipped_empty += 1
                continue

            sha256 = hashlib.sha256(data).hexdigest()
            stored = row["content_hash"]
            if stored and stored != sha256:
                # content_text is NUL-stripped at write time while content_hash
                # covers the ORIGINAL bytes, so a disagreement means the inline
                # copy is lossy. Rewriting the hash would canonize that lossy
                # copy as authoritative — report and leave the row alone.
                counts.skipped_lossy += 1
                logger.warning(
                    "hash mismatch, left inline: %s %s (stored %s, inline %s)",
                    row["workspace_id"],
                    row["file_path"],
                    stored,
                    sha256,
                )
                continue

            if not apply:
                counts.moved += 1
                counts.moved_bytes += len(data)
                continue

            counts.pending.append(
                _Pending(
                    file_id=file_id,
                    user_id=row["user_id"],
                    sha256=sha256,
                    data=data,
                    stored_hash=stored,
                    workspace_id=str(row["workspace_id"]),
                    file_path=row["file_path"],
                )
            )
            counts.pending_bytes += len(data)
            if (
                len(counts.pending) >= UPLOAD_CONCURRENCY
                or counts.pending_bytes >= INFLIGHT_MAX_BYTES
            ):
                await _flush(conn, counts)

        await _flush(conn, counts)
        logger.info(
            "progress: %d moved, %d bytes, %d failed",
            counts.moved,
            counts.moved_bytes,
            counts.failed,
        )

    logger.info(
        "%s %d file(s), %d bytes; %d skipped (lossy inline copy), "
        "%d skipped (raced), %d skipped (no content), "
        "%d skipped (legacy owner), %d failed",
        "moved" if apply else "would move",
        counts.moved,
        counts.moved_bytes,
        counts.skipped_lossy,
        counts.skipped_raced,
        counts.skipped_empty,
        counts.skipped_legacy_owner,
        counts.failed,
    )
    return 1 if counts.failed else 0


def _inline_columns(data: bytes, is_binary: bool) -> tuple[str | None, bytes | None]:
    """Choose the inline columns to restore ``data`` into.

    Rollback is the whole point of this mode, so the row has to be readable by
    the code being rolled back to, which checks ``is_binary`` first and tests
    ``content_binary`` for truthiness.

    Empty files therefore go to TEXT even when binary: ``b""`` is falsy, so the
    old reader skips a BYTEA column holding it and 404s the file. Bytes that
    can't round-trip through TEXT go to BYTEA, and the caller marks those rows
    binary. Postgres rejects NUL in TEXT and a lone invalid byte has no TEXT
    spelling at all; substituting either one writes a lossy copy while
    ``content_hash`` still covers the original bytes, precisely the state
    ``_forward`` refuses to touch. Decoding is therefore strict: a file
    classified as text by a 64KiB NUL scan can still hold invalid UTF-8 further
    in, and ``errors="replace"`` would silently canonize U+FFFD in its place.
    """
    if not data:
        return "", None
    if is_binary:
        return None, data
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return None, data
    if "\x00" in text:
        return None, data
    return text, None


async def _reverse(
    conn: psycopg.AsyncConnection, apply: bool, batch: int, workspace_id: str | None
) -> int:
    where = BLOB_BACKED_SQL
    after: str | None = None
    restored = failed = as_binary = raced = 0
    restored_bytes = 0

    while True:
        ids = await _next_ids(conn, where, after, batch, workspace_id)
        if not ids:
            break
        after = ids[-1]

        for file_id in ids:
            row = await _load_row(conn, file_id)
            if row is None or (row["blob_sha256"] is None and row["pack_sha256"] is None):
                continue
            sha256 = row["blob_sha256"]
            pack_sha256 = row["pack_sha256"]

            if not apply:
                restored += 1
                continue

            expected_len = row["file_size"]
            if sha256 is not None:
                data = await asyncio.to_thread(
                    get_bytes, blob_key(row["user_id"], sha256)
                )
                # A whole-file row's own digest is the object key.
                expected_hash = sha256
            else:
                # A packed row: its bytes are a slice of the chunk, and its
                # own digest is content_hash.
                data = await asyncio.to_thread(
                    get_bytes_range,
                    blob_key(row["user_id"], pack_sha256),
                    int(row["pack_offset"] or 0),
                    int(expected_len or 0),
                )
                expected_hash = row["content_hash"]
            if data is None:
                failed += 1
                logger.error(
                    "blob %s unreadable, left pointing at storage: %s %s",
                    sha256 or pack_sha256,
                    row["workspace_id"],
                    row["file_path"],
                )
                continue
            # Length always, digest whenever the row records one. This write
            # NULLs the only pointer to the good bytes, so bytes that fail
            # either check must stay in storage and be reported: an unverified
            # restore is a silently corrupted file the rollback then keeps.
            if expected_len is not None and len(data) != int(expected_len):
                failed += 1
                logger.error(
                    "blob %s length mismatch, left pointing at storage: %s %s "
                    "(row says %s bytes, read %d)",
                    sha256 or pack_sha256,
                    row["workspace_id"],
                    row["file_path"],
                    expected_len,
                    len(data),
                )
                continue
            if expected_hash is not None:
                actual = hashlib.sha256(data).hexdigest()
                if actual != expected_hash:
                    failed += 1
                    logger.error(
                        "blob %s hash mismatch, left pointing at storage: %s %s "
                        "(got %s)",
                        sha256 or pack_sha256,
                        row["workspace_id"],
                        row["file_path"],
                        actual,
                    )
                    continue

            content_text, content_binary = _inline_columns(data, row["is_binary"])
            # The rollback target reads BYTEA only when is_binary is true, so a
            # NUL-bearing text row put into content_binary has to be relabelled
            # or the code we are rolling back to cannot read it — which would
            # make this whole mode a no-op for exactly the rows it exists for.
            is_binary = row["is_binary"] or content_binary is not None
            if content_binary is not None and not row["is_binary"]:
                as_binary += 1
                logger.warning(
                    "text file is not clean UTF-8 (NUL or invalid bytes), "
                    "restored to BYTEA and marked binary so the rollback "
                    "target can read it: %s %s",
                    row["workspace_id"],
                    row["file_path"],
                )

            # Every column the read observed is in the CAS, not just the two
            # pointers: a repack can hand a different chunk the same digest
            # with different member boundaries, so digest equality alone would
            # let a re-pointed row take a slice cut for its predecessor.
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE workspace_files
                       SET content_text = %s,
                           content_binary = %s,
                           is_binary = %s,
                           blob_sha256 = NULL,
                           pack_sha256 = NULL,
                           pack_offset = NULL
                     WHERE workspace_file_id = %s::uuid
                       AND blob_sha256 IS NOT DISTINCT FROM %s
                       AND pack_sha256 IS NOT DISTINCT FROM %s
                       AND content_hash IS NOT DISTINCT FROM %s
                       AND pack_offset IS NOT DISTINCT FROM %s
                       AND file_size IS NOT DISTINCT FROM %s
                    """,
                    (
                        content_text,
                        content_binary,
                        is_binary,
                        file_id,
                        sha256,
                        pack_sha256,
                        row["content_hash"],
                        row["pack_offset"],
                        row["file_size"],
                    ),
                )
                if cur.rowcount:
                    restored += 1
                    restored_bytes += len(data)
                else:
                    # The CAS missed: a sync re-pointed this row while we held
                    # its bytes. Silence here would be the dangerous outcome —
                    # the row is still blob-backed, and the caller is about to
                    # treat a zero exit as "safe to downgrade".
                    raced += 1
                    logger.warning(
                        "row re-pointed mid-reverse, still blob-backed: %s %s",
                        row["workspace_id"],
                        row["file_path"],
                    )

    logger.info(
        "%s %d file(s) inline, %d bytes; %d as BYTEA (text that is not clean "
        "UTF-8), %d raced, %d failed",
        "restored" if apply else "would restore",
        restored,
        restored_bytes,
        as_binary,
        raced,
        failed,
    )
    if apply:
        left = await _count_blob_backed(conn, workspace_id)
        if left:
            logger.error(
                "%d row(s) still reference storage — DO NOT run the migration "
                "downgrade; re-run --reverse --apply until this reaches zero",
                left,
            )
            return 1
    return 1 if (failed or raced) else 0


async def _count_blob_backed(
    conn: psycopg.AsyncConnection, workspace_id: str | None
) -> int:
    """Rows still pointing at object storage. Zero is the downgrade precondition."""
    sql = f"SELECT count(*) FROM workspace_files WHERE {BLOB_BACKED_SQL}"
    params: tuple = ()
    if workspace_id:
        sql += " AND workspace_id = %s::uuid"
        params = (workspace_id,)
    async with conn.cursor() as cur:
        await cur.execute(sql, params)
        row = await cur.fetchone()
    return row[0] if row else 0


async def _run(
    apply: bool,
    reverse: bool,
    batch: int,
    workspace_id: str | None,
    include_legacy_owners: bool = False,
) -> int:
    if not is_storage_enabled():
        logger.error(
            "Object storage is disabled (STORAGE_PROVIDER=none / "
            "agent_config.yaml storage.provider). Refusing to run."
        )
        return 2
    if not apply:
        logger.info("DRY RUN — nothing will be written. Re-run with --apply.")
    async with await psycopg.AsyncConnection.connect(
        build_db_uri(), autocommit=True
    ) as conn:
        if reverse:
            return await _reverse(conn, apply, batch, workspace_id)
        return await _forward(
            conn, apply, batch, workspace_id, include_legacy_owners
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="Mutate rows (default: dry-run)."
    )
    parser.add_argument(
        "--reverse",
        action="store_true",
        help="Pull blob bytes back into the inline columns (rollback path).",
    )
    parser.add_argument(
        "--batch", type=int, default=500, help="Ids fetched per page (default: 500)."
    )
    parser.add_argument(
        "--workspace",
        metavar="UUID",
        help="Limit to one workspace, so the rollout can be staged and verified "
        "before the whole table moves.",
    )
    parser.add_argument(
        "--include-legacy-owners",
        action="store_true",
        help="Also move rows whose owner has a non-UUID id. Such an account is "
        "re-keyed by its first platform login and its objects would not follow; "
        "pass this once no such login can still arrive, or on a build without "
        "platform auth, where every id is local.",
    )
    args = parser.parse_args()
    return asyncio.run(
        _run(
            args.apply,
            args.reverse,
            args.batch,
            args.workspace,
            args.include_legacy_owners,
        )
    )


if __name__ == "__main__":
    sys.exit(main())
