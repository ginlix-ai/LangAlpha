"""Global content-addressed store for raw per-access tool result bodies.

Bodies are keyed by ``result_sha256`` and shared across users/turns: a static
filing fetched many times is stored once. Bodies up to
``RESULT_BODY_MAX_BYTES`` live inline in Postgres; larger bodies keep a
byte-safe head inline and spill the full bytes to object storage under
``provenance/{sha256}``. Every write is best-effort and never raises — losing a
body must not break the turn that produced it. GC is an independent mark-sweep
against ``provenance_records`` (no FK; ``result_sha256`` is the logical link).
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Iterable

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.utils.pg_sanitize import strip_pg_nul_str
from src.utils.storage import (
    delete_object as _storage_delete_object,
    get_bytes as _storage_get_bytes,
    is_storage_enabled,
    upload_bytes as _storage_upload_bytes,
)

logger = logging.getLogger(__name__)

# Inline/spill cap. Canonical home is ``ptc_agent.agent.provenance.types``;
# re-declared here (not imported) to keep this server-side module free of the
# agent package's import graph and unaffected by the sibling edit adding it
# there. Both copies MUST hold the same value.
RESULT_BODY_MAX_BYTES = 64 * 1024

# Rows per multi-row INSERT. Bounds the statement size (each body_inline is up to
# RESULT_BODY_MAX_BYTES), so a turn with hundreds of MCP calls upserts in a few
# chunks rather than one giant statement.
_BATCH_CHUNK = 50

# Advisory-lock key for the GC sweep. The sweep runs from every app instance's
# lifespan; this lets only one instance sweep per cycle (others skip) if langalpha
# is ever scaled past a single process.
_GC_LOCK_KEY = "provenance_gc_sweep"

# GC grace window (days): an orphan body younger than this is presumed mid-turn
# — its provenance record may not have committed yet — so the sweep never reaps
# it. Single source of truth: both sweep_orphan_bodies and ProvenanceGCService
# resolve their grace from this, and the reuse-touch window below derives from it
# so the two can't drift.
GC_GRACE_DAYS = 7

# Reuse-touch window (days). A dedup write refreshes an existing body's created_at
# ONLY when it's older than this — re-arming the grace window for a body being
# resurrected from near GC-eligibility, so the sweep can't reap it mid-reuse
# before the new turn's provenance record commits. One day inside the grace edge
# so a reused body is always refreshed before it can be swept. Recent rows (the
# overwhelming majority of dedup hits) fail the predicate, so the UPDATE matches
# nothing — no row lock, no rewrite — preserving the no-churn DO NOTHING path.
#
# Why a conditional touch and NOT the obvious `ON CONFLICT DO UPDATE SET
# last_seen_at = NOW()`: this store is global + content-addressed, so a popular
# body (a hot 10-Q fetched by many users) is ONE row that many concurrent turns
# dedup against. DO UPDATE on every hit takes a row write-lock on that hot row
# each time, serializing those turns against each other AND against the GC's
# DELETE on the same rows (a deadlock surface, on exactly the hottest rows). The
# age-gated touch keeps the common path lock-free and only locks the rare row
# that is actually near GC-eligibility.
_REUSE_TOUCH_AFTER_DAYS = GC_GRACE_DAYS - 1

# A real SHA-256 hexdigest. Object keys are derived from the sha, so anything that
# isn't a clean digest (e.g. a path-traversal payload from a future untrusted
# caller) must never reach the object store as ``provenance/{sha}``.
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")

# Ceiling on a single ``full=true`` body read. Spilled objects are already bounded
# by the storage upload cap (default 10 MiB), but that whole body would otherwise
# be decoded into one string and serialized into one JSON response on every
# request. Cap the read so a verifier pull can't materialize ~10 MiB per call;
# bodies past the cap come back truncated (true byte_len preserved, verified=false).
FULL_BODY_READ_MAX_BYTES = 4 * 1024 * 1024


def _build_body_row(
    sha256: str, body: str, true_byte_len: int, content_type: str | None
) -> tuple | None:
    """Resolve one (sha, body) into an inline-head row, spilling oversize bytes.

    Returns the row tuple to insert, or None when there is nothing to store.
    Object-storage upload (for bodies over the cap) happens here, off the DB txn.
    """
    if not sha256 or not body:
        return None
    encoded = body.encode("utf-8")
    object_key: str | None = None
    if len(encoded) <= RESULT_BODY_MAX_BYTES:
        body_inline = body
    else:
        # Slice on a byte boundary then decode with errors="ignore" so a
        # multibyte char split at the cap is dropped, not mojibake.
        body_inline = encoded[:RESULT_BODY_MAX_BYTES].decode("utf-8", errors="ignore")
        # Only spill under a well-formed digest key; a malformed sha keeps the
        # inline head and skips object storage rather than writing a junk key.
        if is_storage_enabled() and _SHA256_RE.match(sha256):
            key = f"provenance/{sha256}"
            uploaded = _storage_upload_bytes(key, encoded, content_type)
            if uploaded:
                object_key = key
    return (
        strip_pg_nul_str(sha256),
        strip_pg_nul_str(body_inline),
        strip_pg_nul_str(object_key),
        true_byte_len,
        strip_pg_nul_str(content_type),
    )


async def store_result_bodies(
    items: Iterable[tuple[str, str, int, str | None]],
) -> None:
    """Batch-persist content-addressed result bodies (best-effort, never raises).

    ``items`` is an iterable of ``(sha256, body, true_byte_len, content_type)``.
    Dedupes by sha in-memory (a single turn's market fan-out repeats the same body
    across symbols), spills oversize bodies to object storage, then upserts the
    rows in chunked multi-row statements on ONE connection. Bodies are immutable +
    content-addressed, so the write is ``ON CONFLICT DO NOTHING`` — a dedup hit is a
    pure no-op (no row rewrite, no WAL, no autovacuum churn on hot rows).

    First-writer-wins on the body is benign: a collision means byte-identical
    *unredacted* content (same sha), and secret-bearing output is execution-
    specific (unique sha), so the shared body is never another tenant's secret;
    differing redaction across tenants only affects which redacted-but-equivalent
    bytes are served, flagged by ``verified=false`` downstream.
    """
    prepared: dict[str, tuple] = {}
    for sha256, body, true_byte_len, content_type in items:
        if not sha256 or not body or sha256 in prepared:
            continue
        prepared[sha256] = (body, true_byte_len, content_type)
    if not prepared:
        return
    # Object keys uploaded this call (only oversize spills set one). Tracked so a
    # DB failure can reclaim uploads whose row never committed — the upload in
    # _build_body_row precedes the insert, and GC only learns objects from
    # deleted rows, so an uncommitted upload would otherwise be invisible to it.
    uploaded: list[tuple[str, str]] = []
    committed: set[str] = set()
    try:
        rows = []
        for sha256, (body, true_byte_len, content_type) in prepared.items():
            # Build small inline bodies (the common case) synchronously on the
            # event loop — pure encode + NUL-scan, no I/O — so we don't pay a
            # thread-pool hop per row. Only an oversize body reaches boto3 (the
            # spill branch in _build_body_row); offload just that off the loop.
            try:
                if len(body.encode("utf-8")) > RESULT_BODY_MAX_BYTES:
                    row = await asyncio.to_thread(
                        _build_body_row, sha256, body, true_byte_len, content_type
                    )
                else:
                    row = _build_body_row(sha256, body, true_byte_len, content_type)
            except Exception as e:
                # Isolate per-item build failures so one bad body can't drop the
                # rest of the turn's. _build_body_row is already defensive
                # (upload_bytes returns False, never raises), so this is
                # belt-and-suspenders for the module's "best-effort" contract.
                logger.warning(
                    "[provenance] skipped one result body (build failed)",
                    extra={"result_sha256": sha256, "error": str(e)},
                )
                continue
            if row is not None:
                rows.append(row)
                if row[2]:
                    uploaded.append((row[0], row[2]))
        if not rows:
            return
        shas = [row[0] for row in rows]
        async with get_db_connection() as conn:
            # One transaction for the whole turn's bodies: the reuse-touch UPDATE
            # and every INSERT chunk commit together in a single fsync, instead of
            # one commit per chunk on the autocommit pool. On a raise the block
            # rolls back as a unit, so `committed` stays empty and every spilled
            # upload is reclaimed in the except below.
            async with conn.cursor() as cur, conn.transaction():
                # Reuse touch (D′): re-arm the GC grace window for any EXISTING
                # body being reused here that has aged near eligibility, so the
                # sweep can't delete it mid-reuse before this turn's provenance
                # record commits. Conditional on age, so recent rows (most dedup
                # hits) and not-yet-inserted shas match nothing — no lock, no
                # rewrite. Runs before the insert; brand-new shas are then created
                # fresh by the DO NOTHING insert below. See sweep_orphan_bodies.
                await cur.execute(
                    """
                    UPDATE provenance_result_bodies
                       SET created_at = NOW()
                     WHERE result_sha256 = ANY(%s)
                       AND created_at < NOW() - make_interval(days => %s)
                    """,
                    (shas, _REUSE_TOUCH_AFTER_DAYS),
                )
                for start in range(0, len(rows), _BATCH_CHUNK):
                    chunk = rows[start : start + _BATCH_CHUNK]
                    values = ", ".join(["(%s, %s, %s, %s, %s)"] * len(chunk))
                    params = [v for row in chunk for v in row]
                    await cur.execute(
                        f"""
                        INSERT INTO provenance_result_bodies
                            (result_sha256, body_inline, object_key, byte_len, content_type)
                        VALUES {values}
                        ON CONFLICT (result_sha256) DO NOTHING
                        """,
                        params,
                    )
            # Transaction committed here: every chunk is now durable, so all
            # spilled uploads have a row. (A raise skips this — `committed` stays
            # empty and the except reclaims the uploads.)
            committed.update(shas)
    except Exception as e:
        logger.warning(f"[provenance] store_result_bodies failed ({len(prepared)} items): {e}")
        # Reclaim spilled objects whose row never committed (best-effort). A
        # later store of the same content re-uploads + inserts, so a missed one
        # self-heals; this just bounds orphan accumulation on a hard DB failure.
        for sha256, object_key in uploaded:
            if sha256 in committed:
                continue
            try:
                await asyncio.to_thread(_storage_delete_object, object_key)
            except Exception:
                logger.debug(
                    "[provenance] orphan upload cleanup failed (harmless)",
                    extra={"object_key": object_key},
                )


async def store_result_body(
    sha256: str,
    body: str,
    true_byte_len: int,
    content_type: str | None = None,
) -> None:
    """Single-body convenience wrapper over :func:`store_result_bodies`."""
    await store_result_bodies([(sha256, body, true_byte_len, content_type)])


async def fetch_result_bodies(conn, shas: list[str]) -> dict[str, dict]:
    """Fetch body rows for ``shas`` keyed by sha (inline/metadata only).

    Does NOT fetch spilled objects — the caller decides whether to read the full
    body via ``fetch_full_body``. Empty input returns an empty dict.
    """
    if not shas:
        return {}
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT result_sha256, body_inline, object_key, byte_len, content_type
            FROM provenance_result_bodies
            WHERE result_sha256 = ANY(%s)
            """,
            (list(shas),),
        )
        rows = await cur.fetchall()
    return {
        row["result_sha256"]: {
            "body_inline": row["body_inline"],
            "object_key": row["object_key"],
            "byte_len": row["byte_len"],
            "content_type": row["content_type"],
        }
        for row in rows
    }


async def fetch_full_body(
    sha256: str,
    max_bytes: int = FULL_BODY_READ_MAX_BYTES,
    *,
    row: dict | None = None,
) -> str | None:
    """Return the full body for one sha, reading the spilled object if present.

    Capped at ``max_bytes`` (byte-sliced before decode, so the caller's response
    stays bounded even for a ~10 MiB spilled object); an over-cap body comes back
    head-only and the caller's ``truncated`` check flips on the true ``byte_len``.
    Best-effort: returns None on any failure or when the sha is unknown.

    Pass ``row`` (a dict carrying ``body_inline`` + ``object_key``, e.g. from
    :func:`fetch_result_bodies`) to skip the row lookup — the caller already holds
    those columns, so the only work left is the optional spilled-object read.
    """
    if not sha256:
        return None
    try:
        if row is None:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(
                        """
                        SELECT body_inline, object_key
                        FROM provenance_result_bodies
                        WHERE result_sha256 = %s
                        """,
                        (sha256,),
                    )
                    row = await cur.fetchone()
            if row is None:
                return None
        object_key = row["object_key"]
        if object_key and is_storage_enabled():
            data = await asyncio.to_thread(_storage_get_bytes, object_key)
            if data is not None:
                return data[:max_bytes].decode("utf-8", errors="ignore")
        return row["body_inline"]
    except Exception as e:
        logger.warning(f"[provenance] fetch_full_body failed for sha={sha256}: {e}")
        return None


async def sweep_orphan_bodies(grace_days: int = GC_GRACE_DAYS) -> int:
    """Delete body rows unreferenced by any provenance record past the grace window.

    Mark-sweep GC: removes rows whose ``result_sha256`` no longer appears in
    ``provenance_records`` and whose ``created_at`` is older than ``grace_days``.
    Grace on ``created_at`` covers the body-write → record-commit gap; a body
    reused from near-eligibility has its ``created_at`` refreshed by the reuse
    touch in :func:`store_result_bodies` (D′), so it can't be reaped mid-reuse.
    A non-blocking advisory lock keeps concurrent instances from running
    duplicate sweeps.

    Spilled objects are reclaimed AFTER the row delete commits, and only for shas
    a concurrent reuse hasn't re-inserted in the meantime: the object key is
    content-addressed (``provenance/{sha}``), so deleting it out from under a
    freshly re-inserted row would strand that live row's spilled body. Returns
    rows deleted.
    """
    try:
        rows: list = []
        to_delete: list[str] = []
        async with get_db_connection() as conn:
            # Explicit txn so the xact-scoped advisory lock spans the DELETE on the
            # autocommit pool; non-blocking try-lock means a second instance skips
            # this cycle rather than waiting and then re-running an empty DELETE.
            async with conn.cursor() as cur, conn.transaction():
                await cur.execute(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))",
                    (_GC_LOCK_KEY,),
                )
                locked = await cur.fetchone()
                if not (locked and locked[0]):
                    logger.debug("[provenance] sweep skipped — GC lock held elsewhere")
                    return 0
                # make_interval(days => %s) binds cleanly; a bare INTERVAL '%s days'
                # literal can't be parameterized by psycopg3.
                await cur.execute(
                    """
                    DELETE FROM provenance_result_bodies b
                    WHERE NOT EXISTS (
                        SELECT 1 FROM provenance_records p
                        WHERE p.result_sha256 = b.result_sha256
                    )
                      AND b.created_at < NOW() - make_interval(days => %s)
                    RETURNING result_sha256, object_key
                    """,
                    (grace_days,),
                )
                rows = await cur.fetchall()

            # Re-check (post-commit, on the same conn) which spilled shas are truly
            # gone; a concurrent reuse may have re-inserted one with the same
            # content-addressed object key, and deleting that object would strand
            # the live row's body. Only the survivors get their objects reclaimed.
            # Residual (accepted): a reuse that uploaded provenance/{sha} but hasn't
            # committed its insert when this recheck runs is still seen as absent,
            # so its object can be deleted out from under the about-to-commit row.
            # That row then degrades to head-only on full=true (inline head still
            # served) and self-heals on the next reuse, which re-uploads
            # unconditionally — so it's a transient degrade, never data loss.
            spilled = [(sha, key) for sha, key in rows if key]
            if spilled:
                async with conn.cursor() as check_cur:
                    await check_cur.execute(
                        "SELECT result_sha256 FROM provenance_result_bodies "
                        "WHERE result_sha256 = ANY(%s)",
                        ([sha for sha, _ in spilled],),
                    )
                    present = {r[0] for r in await check_cur.fetchall()}
                to_delete = [key for sha, key in spilled if sha not in present]

        deleted = len(rows)
        # Slow object deletes run after the connection is released to the pool.
        for object_key in to_delete:
            try:
                await asyncio.to_thread(_storage_delete_object, object_key)
            except Exception:
                logger.warning(
                    "[provenance] orphan object delete failed (harmless)",
                    extra={"object_key": object_key},
                )
        return deleted
    except Exception as e:
        logger.warning(f"[provenance] sweep_orphan_bodies failed: {e}")
        return 0
