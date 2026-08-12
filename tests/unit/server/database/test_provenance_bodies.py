"""Content-addressed result-body store (``provenance_bodies``).

Mirrors the mock-connection approach of ``test_provenance_insert.py`` /
``test_market_insight_db.py``: ``store_result_body`` / ``sweep_orphan_bodies``
open their OWN pooled connection via ``get_db_connection``, so we patch that at
the module's import path rather than passing a conn in. The object-storage layer
(``upload_bytes`` / ``get_bytes`` / ``delete_object`` / ``is_storage_enabled``)
is mocked where ``provenance_bodies`` imported it. Asserts:

* small body → inline, no spill, no upload, byte_len == true length,
* body > 64 KiB + storage → byte-safe head inline + full bytes uploaded to
  ``provenance/{sha}`` + object_key set + byte_len is the TRUE full length,
* body > 64 KiB without a bucket → inline head only, no upload (graceful),
* ``ON CONFLICT (result_sha256) DO NOTHING`` dedups without rewriting the
  immutable body (a hit is a pure no-op — no last_seen bump),
* ``store_result_bodies`` batches a turn's writes: dedups by sha, chunks the
  multi-row upsert into one connection,
* truncation is recoverable from ``byte_len`` vs the inline length,
* best-effort: empty inputs no-op, a raising storage/DB layer never propagates,
* GC deletes only unreferenced + past-grace rows and ``delete_object``s the
  swept spills, surviving a raising delete.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.provenance_bodies import (
    FULL_BODY_READ_MAX_BYTES,
    RESULT_BODY_MAX_BYTES,
    fetch_full_body,
    fetch_result_bodies,
    store_result_bodies,
    store_result_body,
    sweep_orphan_bodies,
)

MOD = "src.server.database.provenance_bodies"
SHA = "a" * 64


@pytest.fixture
def mock_cursor():
    """AsyncMock cursor; fetchall defaults to no swept rows, fetchone to a
    granted advisory lock ``(True,)`` (the sweep's first fetchone is the lock probe)."""
    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[])
    cursor.fetchone = AsyncMock(return_value=(True,))
    return cursor


@pytest.fixture
def mock_connection(mock_cursor):
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(*args, **kwargs):
        yield mock_cursor

    @asynccontextmanager
    async def _txn_cm(*args, **kwargs):
        yield None

    conn.cursor = _cursor_cm
    conn.transaction = _txn_cm
    return conn


@pytest.fixture
def patched_db(mock_connection):
    """Patch get_db_connection at the provenance_bodies import path."""

    @asynccontextmanager
    async def _fake():
        yield mock_connection

    with patch(f"{MOD}.get_db_connection", new=_fake):
        yield mock_connection


def _insert_params(mock_cursor):
    """Params of the INSERT INTO provenance_result_bodies execute call."""
    call = next(
        c
        for c in mock_cursor.execute.call_args_list
        if "INSERT INTO provenance_result_bodies" in c.args[0]
    )
    return call.args[1]


def _insert_field(mock_cursor, name):
    """Bind value by column name (INSERT column order is fixed in the module)."""
    cols = ["result_sha256", "body_inline", "object_key", "byte_len", "content_type"]
    return _insert_params(mock_cursor)[cols.index(name)]


class TestStoreResultBodyInline:
    @pytest.mark.asyncio
    async def test_small_body_stored_inline_no_spill(self, patched_db, mock_cursor):
        body = "small result body"
        with (
            patch(f"{MOD}.is_storage_enabled", return_value=True),
            patch(f"{MOD}._storage_upload_bytes") as upload,
        ):
            await store_result_body(SHA, body, true_byte_len=len(body.encode()))

        # Inline body is the body verbatim; no spill, no upload.
        assert _insert_field(mock_cursor, "body_inline") == body
        assert _insert_field(mock_cursor, "object_key") is None
        upload.assert_not_called()
        # byte_len carries the TRUE full length the caller passed.
        assert _insert_field(mock_cursor, "byte_len") == len(body.encode())

    @pytest.mark.asyncio
    async def test_exactly_at_cap_stays_inline(self, patched_db, mock_cursor):
        # Boundary: a body whose encoded length == cap must NOT spill.
        body = "x" * RESULT_BODY_MAX_BYTES
        with (
            patch(f"{MOD}.is_storage_enabled", return_value=True),
            patch(f"{MOD}._storage_upload_bytes") as upload,
        ):
            await store_result_body(SHA, body, true_byte_len=len(body.encode()))
        assert _insert_field(mock_cursor, "body_inline") == body
        assert _insert_field(mock_cursor, "object_key") is None
        upload.assert_not_called()

    @pytest.mark.asyncio
    async def test_content_type_threaded_through(self, patched_db, mock_cursor):
        body = "ok"
        with (
            patch(f"{MOD}.is_storage_enabled", return_value=True),
            patch(f"{MOD}._storage_upload_bytes"),
        ):
            await store_result_body(
                SHA, body, true_byte_len=2, content_type="application/json"
            )
        assert _insert_field(mock_cursor, "content_type") == "application/json"


class TestStoreResultBodySpill:
    @pytest.mark.asyncio
    async def test_large_body_spills_head_inline_full_to_object(
        self, patched_db, mock_cursor
    ):
        # 100 KiB > 64 KiB cap → byte-safe head inline, full bytes to storage.
        full = "y" * (100 * 1024)
        full_bytes = full.encode()
        with (
            patch(f"{MOD}.is_storage_enabled", return_value=True),
            patch(f"{MOD}._storage_upload_bytes", return_value=True) as upload,
        ):
            await store_result_body(
                SHA, full, true_byte_len=len(full_bytes), content_type="text/plain"
            )

        inline = _insert_field(mock_cursor, "body_inline")
        # Head is the first 64 KiB, byte-safe (single-byte chars here so == cap).
        assert inline == full[:RESULT_BODY_MAX_BYTES]
        assert len(inline.encode()) <= RESULT_BODY_MAX_BYTES
        # Full bytes uploaded to the content-addressed key, with content_type.
        upload.assert_called_once_with(f"provenance/{SHA}", full_bytes, "text/plain")
        # object_key set only because upload reported success.
        assert _insert_field(mock_cursor, "object_key") == f"provenance/{SHA}"
        # byte_len is the TRUE full length, not the inline head length.
        assert _insert_field(mock_cursor, "byte_len") == len(full_bytes)
        assert _insert_field(mock_cursor, "byte_len") > len(inline.encode())

    @pytest.mark.asyncio
    async def test_head_is_byte_safe_for_multibyte_chars(
        self, patched_db, mock_cursor
    ):
        # A multibyte char straddling the cap must be dropped, not mojibake.
        # "€" is 3 bytes; fill so the boundary lands mid-char.
        body = "a" * (RESULT_BODY_MAX_BYTES - 1) + "€" + "b" * 1000
        with (
            patch(f"{MOD}.is_storage_enabled", return_value=True),
            patch(f"{MOD}._storage_upload_bytes", return_value=True),
        ):
            await store_result_body(SHA, body, true_byte_len=len(body.encode()))
        inline = _insert_field(mock_cursor, "body_inline")
        # No replacement chars / partial bytes — clean decode under the cap.
        assert len(inline.encode()) <= RESULT_BODY_MAX_BYTES
        assert "�" not in inline
        assert inline == "a" * (RESULT_BODY_MAX_BYTES - 1)

    @pytest.mark.asyncio
    async def test_malformed_sha_skips_spill_no_object_key(
        self, patched_db, mock_cursor
    ):
        # An oversize body keyed by a non-hex sha (e.g. a traversal payload from a
        # future untrusted caller) keeps the inline head and never builds an object
        # key like provenance/../foo.
        full = "y" * (100 * 1024)
        with (
            patch(f"{MOD}.is_storage_enabled", return_value=True),
            patch(f"{MOD}._storage_upload_bytes", return_value=True) as upload,
        ):
            await store_result_body(
                "../evil", full, true_byte_len=len(full.encode())
            )
        upload.assert_not_called()
        assert _insert_field(mock_cursor, "object_key") is None

    @pytest.mark.asyncio
    async def test_upload_failure_leaves_object_key_none(
        self, patched_db, mock_cursor
    ):
        # Storage enabled but upload returns falsey → inline head only, no key.
        full = "z" * (80 * 1024)
        with (
            patch(f"{MOD}.is_storage_enabled", return_value=True),
            patch(f"{MOD}._storage_upload_bytes", return_value=False),
        ):
            await store_result_body(SHA, full, true_byte_len=len(full.encode()))
        assert _insert_field(mock_cursor, "object_key") is None
        # byte_len still reflects the true length (truncation still inferable).
        assert _insert_field(mock_cursor, "byte_len") == len(full.encode())


class TestStoreResultBodyNoBucket:
    @pytest.mark.asyncio
    async def test_large_body_no_bucket_degrades_to_inline_head(
        self, patched_db, mock_cursor
    ):
        # No storage configured → inline head only, object_key NULL, no upload.
        full = "w" * (200 * 1024)
        with (
            patch(f"{MOD}.is_storage_enabled", return_value=False),
            patch(f"{MOD}._storage_upload_bytes") as upload,
        ):
            await store_result_body(SHA, full, true_byte_len=len(full.encode()))
        upload.assert_not_called()
        assert _insert_field(mock_cursor, "object_key") is None
        inline = _insert_field(mock_cursor, "body_inline")
        assert len(inline.encode()) <= RESULT_BODY_MAX_BYTES
        # Truncation remains recoverable from byte_len > inline length.
        assert _insert_field(mock_cursor, "byte_len") > len(inline.encode())


class TestStoreResultBodyConflict:
    @pytest.mark.asyncio
    async def test_insert_uses_on_conflict_do_nothing(
        self, patched_db, mock_cursor
    ):
        # Bodies are immutable + content-addressed, so a dedup hit must be a pure
        # no-op: ON CONFLICT DO NOTHING (no row rewrite, no last_seen bump, no WAL).
        with patch(f"{MOD}.is_storage_enabled", return_value=False):
            await store_result_body(SHA, "body", true_byte_len=4)
        sql = next(
            c.args[0]
            for c in mock_cursor.execute.call_args_list
            if "INSERT INTO provenance_result_bodies" in c.args[0]
        )
        assert "ON CONFLICT (result_sha256) DO NOTHING" in sql
        # The conflict clause must NOT rewrite any stored column.
        conflict = sql.split("ON CONFLICT", 1)[1]
        assert "DO UPDATE" not in conflict
        assert "body_inline" not in conflict
        assert "object_key" not in conflict

    @pytest.mark.asyncio
    async def test_reuse_touch_precedes_insert_and_is_age_conditional(
        self, patched_db, mock_cursor
    ):
        # D′: a conditional created_at bump runs BEFORE the DO NOTHING insert, so a
        # reused old body's grace window is re-armed and the sweep can't reap it
        # mid-reuse. Conditional on age → recent rows match nothing (no churn).
        from src.server.database.provenance_bodies import _REUSE_TOUCH_AFTER_DAYS

        with patch(f"{MOD}.is_storage_enabled", return_value=False):
            await store_result_body(SHA, "body", true_byte_len=4)
        calls = mock_cursor.execute.call_args_list
        update_idx = next(
            i for i, c in enumerate(calls)
            if "UPDATE provenance_result_bodies" in c.args[0]
        )
        insert_idx = next(
            i for i, c in enumerate(calls)
            if "INSERT INTO provenance_result_bodies" in c.args[0]
        )
        assert update_idx < insert_idx  # touch first, then insert
        update_sql = calls[update_idx].args[0]
        assert "SET created_at = NOW()" in update_sql
        assert "created_at < NOW() - make_interval(days =>" in update_sql
        # shas bound as an array, age threshold bound (never interpolated).
        assert calls[update_idx].args[1] == ([SHA], _REUSE_TOUCH_AFTER_DAYS)

    @pytest.mark.asyncio
    async def test_storing_same_sha_twice_is_single_insert_each(
        self, patched_db, mock_cursor
    ):
        # Two stores → two INSERT executes that both rely on ON CONFLICT to keep
        # exactly one row (the DB enforces single-row; we assert the same key +
        # same body are written, so a re-store can't change the stored body).
        with patch(f"{MOD}.is_storage_enabled", return_value=False):
            await store_result_body(SHA, "body-A", true_byte_len=6)
            await store_result_body(SHA, "body-A", true_byte_len=6)
        inserts = [
            c
            for c in mock_cursor.execute.call_args_list
            if "INSERT INTO provenance_result_bodies" in c.args[0]
        ]
        assert len(inserts) == 2
        # Same sha bound both times; body unchanged across the re-store.
        assert inserts[0].args[1][0] == inserts[1].args[1][0] == SHA
        assert inserts[0].args[1][1] == inserts[1].args[1][1] == "body-A"


class TestStoreResultBodiesBatch:
    """The batch writer: dedup by sha, chunked multi-row upserts, one connection."""

    @pytest.mark.asyncio
    async def test_dedups_repeated_sha_to_one_row(self, patched_db, mock_cursor):
        # Market fan-out repeats the same (sha, body) across symbols; the batch
        # must collapse them to a single row rather than re-upserting N times.
        items = [(SHA, "body", 4, None)] * 5
        with patch(f"{MOD}.is_storage_enabled", return_value=False):
            await store_result_bodies(items)
        inserts = [
            c
            for c in mock_cursor.execute.call_args_list
            if "INSERT INTO provenance_result_bodies" in c.args[0]
        ]
        assert len(inserts) == 1
        # One row → exactly one 5-column group bound.
        assert len(inserts[0].args[1]) == 5
        assert inserts[0].args[1][0] == SHA

    @pytest.mark.asyncio
    async def test_distinct_shas_one_multirow_statement(
        self, patched_db, mock_cursor
    ):
        items = [(f"{i:064d}", f"body-{i}", 6, None) for i in range(3)]
        with patch(f"{MOD}.is_storage_enabled", return_value=False):
            await store_result_bodies(items)
        inserts = [
            c
            for c in mock_cursor.execute.call_args_list
            if "INSERT INTO provenance_result_bodies" in c.args[0]
        ]
        assert len(inserts) == 1
        sql = inserts[0].args[0]
        # Three VALUES groups in one statement, DO NOTHING, 15 flat params.
        assert sql.count("(%s, %s, %s, %s, %s)") == 3
        assert "ON CONFLICT (result_sha256) DO NOTHING" in sql
        assert len(inserts[0].args[1]) == 15

    @pytest.mark.asyncio
    async def test_large_batch_is_chunked(self, patched_db, mock_cursor):
        import math

        from src.server.database.provenance_bodies import _BATCH_CHUNK

        n = _BATCH_CHUNK * 2 + 10
        items = [(f"{i:064d}", "b", 1, None) for i in range(n)]
        with patch(f"{MOD}.is_storage_enabled", return_value=False):
            await store_result_bodies(items)
        inserts = [
            c
            for c in mock_cursor.execute.call_args_list
            if "INSERT INTO provenance_result_bodies" in c.args[0]
        ]
        assert len(inserts) == math.ceil(n / _BATCH_CHUNK)

    @pytest.mark.asyncio
    async def test_all_empty_items_no_execute(self, patched_db, mock_cursor):
        await store_result_bodies([("", "b", 1, None), (SHA, "", 0, None)])
        mock_cursor.execute.assert_not_called()


class TestStoreResultBodyBestEffort:
    @pytest.mark.asyncio
    async def test_empty_sha_is_noop(self, patched_db, mock_cursor):
        await store_result_body("", "body", true_byte_len=4)
        mock_cursor.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_body_is_noop(self, patched_db, mock_cursor):
        await store_result_body(SHA, "", true_byte_len=0)
        mock_cursor.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_raising_db_layer_does_not_propagate(self, mock_connection):
        # A DB failure mid-store must be swallowed (the turn must not break).
        @asynccontextmanager
        async def _boom():
            raise RuntimeError("db down")
            yield  # pragma: no cover

        with (
            patch(f"{MOD}.get_db_connection", new=_boom),
            patch(f"{MOD}.is_storage_enabled", return_value=False),
        ):
            await store_result_body(SHA, "body", true_byte_len=4)  # no raise

    @pytest.mark.asyncio
    async def test_raising_storage_layer_does_not_propagate(
        self, patched_db, mock_cursor
    ):
        full = "q" * (70 * 1024)
        with (
            patch(f"{MOD}.is_storage_enabled", return_value=True),
            patch(
                f"{MOD}._storage_upload_bytes",
                side_effect=RuntimeError("s3 down"),
            ),
        ):
            await store_result_body(SHA, full, true_byte_len=len(full.encode()))
        # Upload raised before the INSERT, so no row is written, but no raise.
        assert not any(
            "INSERT INTO provenance_result_bodies" in c.args[0]
            for c in mock_cursor.execute.call_args_list
        )


class TestFetchResultBodies:
    @pytest.mark.asyncio
    async def test_empty_shas_short_circuits(self, mock_connection, mock_cursor):
        out = await fetch_result_bodies(mock_connection, [])
        assert out == {}
        mock_cursor.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_round_trip_truncation_semantics(
        self, mock_connection, mock_cursor
    ):
        # The verifier reads byte_len + inline; truncation is byte_len > inline
        # length. Inline row: byte_len == len(inline) → NOT truncated. Spilled
        # row: byte_len > len(inline) → truncated.
        inline_body = "short inline body"
        head = "h" * RESULT_BODY_MAX_BYTES
        mock_cursor.fetchall.return_value = [
            {
                "result_sha256": "sha-inline",
                "body_inline": inline_body,
                "object_key": None,
                "byte_len": len(inline_body.encode()),
                "content_type": None,
            },
            {
                "result_sha256": "sha-spill",
                "body_inline": head,
                "object_key": "provenance/sha-spill",
                "byte_len": 100 * 1024,
                "content_type": "text/plain",
            },
        ]
        out = await fetch_result_bodies(
            mock_connection, ["sha-inline", "sha-spill"]
        )
        assert set(out) == {"sha-inline", "sha-spill"}

        inline = out["sha-inline"]
        assert inline["object_key"] is None
        # NOT truncated: byte_len equals the inline byte length.
        assert inline["byte_len"] == len(inline["body_inline"].encode())

        spill = out["sha-spill"]
        assert spill["object_key"] == "provenance/sha-spill"
        # Truncated: true byte_len exceeds the stored 64 KiB head.
        assert spill["byte_len"] > len(spill["body_inline"].encode())

    @pytest.mark.asyncio
    async def test_uses_any_array_bind(self, mock_connection, mock_cursor):
        await fetch_result_bodies(mock_connection, ["s1", "s2"])
        call = mock_cursor.execute.call_args
        assert "= ANY(%s)" in call.args[0]
        # Shas are bound as a list (psycopg3 array), not interpolated.
        assert call.args[1] == (["s1", "s2"],)


class TestFetchFullBody:
    @pytest.mark.asyncio
    async def test_unknown_sha_returns_none(self, patched_db, mock_cursor):
        mock_cursor.fetchone.return_value = None
        assert await fetch_full_body(SHA) is None

    @pytest.mark.asyncio
    async def test_empty_sha_returns_none(self, patched_db, mock_cursor):
        assert await fetch_full_body("") is None
        mock_cursor.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_inline_row_returns_inline_no_storage_read(
        self, patched_db, mock_cursor
    ):
        mock_cursor.fetchone.return_value = {
            "body_inline": "the full small body",
            "object_key": None,
        }
        with patch(f"{MOD}._storage_get_bytes") as get_bytes:
            out = await fetch_full_body(SHA)
        assert out == "the full small body"
        get_bytes.assert_not_called()

    @pytest.mark.asyncio
    async def test_spilled_row_reads_full_object(self, patched_db, mock_cursor):
        mock_cursor.fetchone.return_value = {
            "body_inline": "h" * RESULT_BODY_MAX_BYTES,  # only the head
            "object_key": "provenance/sha-spill",
        }
        full = "full body " * 100000
        with (
            patch(f"{MOD}.is_storage_enabled", return_value=True),
            patch(
                f"{MOD}._storage_get_bytes", return_value=full.encode()
            ) as get_bytes,
        ):
            out = await fetch_full_body(SHA)
        # The reassembled body is the full object, not the truncated head.
        assert out == full
        get_bytes.assert_called_once_with("provenance/sha-spill")

    @pytest.mark.asyncio
    async def test_object_read_failure_falls_back_to_head(
        self, patched_db, mock_cursor
    ):
        # If the object is gone/unreadable, return the inline head, never raise.
        mock_cursor.fetchone.return_value = {
            "body_inline": "head only",
            "object_key": "provenance/missing",
        }
        with (
            patch(f"{MOD}.is_storage_enabled", return_value=True),
            patch(f"{MOD}._storage_get_bytes", return_value=None),
        ):
            assert await fetch_full_body(SHA) == "head only"

    @pytest.mark.asyncio
    async def test_spilled_object_capped_at_max_bytes(self, patched_db, mock_cursor):
        # The read is byte-sliced at max_bytes so one request can't decode a whole
        # ~10 MiB object; errors="ignore" drops a multibyte char split at the cut.
        mock_cursor.fetchone.return_value = {
            "body_inline": "head",
            "object_key": "provenance/big",
        }
        full = "é" * 10  # 'é' is 2 bytes in UTF-8 → 20 bytes total
        with (
            patch(f"{MOD}.is_storage_enabled", return_value=True),
            patch(f"{MOD}._storage_get_bytes", return_value=full.encode()),
        ):
            out = await fetch_full_body(SHA, max_bytes=5)
        # 5-byte cut lands mid-char; the dangling half-'é' is dropped → 2 chars.
        assert out == "éé"
        assert len(out.encode()) <= 5

    @pytest.mark.asyncio
    async def test_default_cap_bounds_oversized_object(self, patched_db, mock_cursor):
        # Default cap is FULL_BODY_READ_MAX_BYTES — an object past it comes back
        # truncated to the cap (the caller's truncated flag flips on byte_len).
        mock_cursor.fetchone.return_value = {
            "body_inline": "head",
            "object_key": "provenance/huge",
        }
        oversized = b"x" * (FULL_BODY_READ_MAX_BYTES + 4096)
        with (
            patch(f"{MOD}.is_storage_enabled", return_value=True),
            patch(f"{MOD}._storage_get_bytes", return_value=oversized),
        ):
            out = await fetch_full_body(SHA)
        assert len(out.encode()) == FULL_BODY_READ_MAX_BYTES

    @pytest.mark.asyncio
    async def test_preloaded_row_inline_skips_db_query(self, patched_db, mock_cursor):
        # A caller that already holds the row (e.g. from fetch_result_bodies)
        # passes it via row= — fetch_full_body must not open a second connection.
        row = {"body_inline": "the full small body", "object_key": None}
        with patch(f"{MOD}._storage_get_bytes") as get_bytes:
            out = await fetch_full_body(SHA, row=row)
        assert out == "the full small body"
        mock_cursor.execute.assert_not_called()
        get_bytes.assert_not_called()

    @pytest.mark.asyncio
    async def test_preloaded_row_still_reads_spilled_object(
        self, patched_db, mock_cursor
    ):
        # With a preloaded row the DB query is skipped, but a spilled object_key
        # still triggers the object read (the only work left for full=true).
        row = {"body_inline": "h" * RESULT_BODY_MAX_BYTES, "object_key": "provenance/x"}
        full = "full body " * 100000
        with (
            patch(f"{MOD}.is_storage_enabled", return_value=True),
            patch(f"{MOD}._storage_get_bytes", return_value=full.encode()) as get_bytes,
        ):
            out = await fetch_full_body(SHA, row=row)
        assert out == full
        mock_cursor.execute.assert_not_called()
        get_bytes.assert_called_once_with("provenance/x")


class TestSweepOrphanBodies:
    @pytest.mark.asyncio
    async def test_delete_sql_shape_and_grace_bind(self, patched_db, mock_cursor):
        await sweep_orphan_bodies(grace_days=7)
        delete_call = next(
            c
            for c in mock_cursor.execute.call_args_list
            if "DELETE FROM provenance_result_bodies" in c.args[0]
        )
        sql = delete_call.args[0]
        # Mark-sweep: unreferenced (NOT EXISTS against provenance_records) AND
        # past the grace window, returning object_key for spill cleanup.
        assert "NOT EXISTS" in sql
        assert "provenance_records" in sql
        assert "created_at" in sql
        assert "make_interval(days =>" in sql
        # Returns the sha too, so the post-commit re-check can tell which spilled
        # objects are safe to reclaim.
        assert "RETURNING result_sha256, object_key" in sql
        # grace_days is a bound parameter, never string-interpolated.
        assert delete_call.args[1] == (7,)

    @pytest.mark.asyncio
    async def test_acquires_advisory_lock_before_delete(self, patched_db, mock_cursor):
        # Leader election: the non-blocking try-lock probe runs first, the DELETE
        # second, so the xact-scoped lock is held across the sweep.
        await sweep_orphan_bodies()
        calls = mock_cursor.execute.call_args_list
        assert "pg_try_advisory_xact_lock" in calls[0].args[0]
        assert "DELETE FROM provenance_result_bodies" in calls[1].args[0]

    @pytest.mark.asyncio
    async def test_skips_when_lock_not_acquired(self, patched_db, mock_cursor):
        # Another instance holds the GC lock → probe returns (False,); this sweep
        # issues no DELETE and reaps nothing.
        mock_cursor.fetchone.return_value = (False,)
        with patch(f"{MOD}._storage_delete_object") as delete:
            assert await sweep_orphan_bodies() == 0
        assert not any(
            "DELETE FROM provenance_result_bodies" in c.args[0]
            for c in mock_cursor.execute.call_args_list
        )
        delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_deleted_count(self, patched_db, mock_cursor):
        # Rows are (result_sha256, object_key); the one spilled row triggers a
        # re-check (2nd fetchall) — [] means none re-inserted, so it's reclaimed.
        mock_cursor.fetchall.side_effect = [
            [("s1", None), ("s2", None), ("s3", "provenance/x")],
            [],
        ]
        with patch(f"{MOD}._storage_delete_object"):
            assert await sweep_orphan_bodies() == 3

    @pytest.mark.asyncio
    async def test_deletes_objects_only_for_spilled_rows(
        self, patched_db, mock_cursor
    ):
        # Two swept rows spilled (have object_key), one was inline-only (None). The
        # re-check (2nd fetchall) finds none re-inserted, so both objects reclaim.
        mock_cursor.fetchall.side_effect = [
            [("s1", "provenance/sha1"), ("s2", None), ("s3", "provenance/sha2")],
            [],
        ]
        with patch(f"{MOD}._storage_delete_object") as delete:
            deleted = await sweep_orphan_bodies()
        assert deleted == 3
        called_keys = {c.args[0] for c in delete.call_args_list}
        assert called_keys == {"provenance/sha1", "provenance/sha2"}

    @pytest.mark.asyncio
    async def test_recheck_keeps_object_when_sha_reinserted(
        self, patched_db, mock_cursor
    ):
        # A concurrent reuse re-inserted the swept sha (re-check finds it present),
        # so its content-addressed object must NOT be deleted — it backs the live
        # re-inserted row. Count still reflects the rows the DELETE removed.
        mock_cursor.fetchall.side_effect = [
            [("s1", "provenance/sha1")],  # DELETE removed it
            [("s1",)],  # re-check: reused, back in the table
        ]
        with patch(f"{MOD}._storage_delete_object") as delete:
            assert await sweep_orphan_bodies() == 1
        delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_object_delete_failure_does_not_raise(
        self, patched_db, mock_cursor
    ):
        # A failing object delete is harmless — sweep still returns the count.
        mock_cursor.fetchall.side_effect = [[("s1", "provenance/sha1")], []]
        with patch(
            f"{MOD}._storage_delete_object",
            side_effect=RuntimeError("delete failed"),
        ):
            assert await sweep_orphan_bodies() == 1

    @pytest.mark.asyncio
    async def test_db_failure_returns_zero_not_raise(self):
        @asynccontextmanager
        async def _boom():
            raise RuntimeError("db down")
            yield  # pragma: no cover

        with patch(f"{MOD}.get_db_connection", new=_boom):
            assert await sweep_orphan_bodies() == 0

    @pytest.mark.asyncio
    async def test_no_orphans_no_object_deletes(self, patched_db, mock_cursor):
        # Default fetchall is [] → nothing swept, delete_object never called.
        with patch(f"{MOD}._storage_delete_object") as delete:
            assert await sweep_orphan_bodies() == 0
        delete.assert_not_called()


class TestLeanIndexWriterHasNoResultFull:
    def test_insert_columns_has_no_result_full(self):
        # The index writer was reverted to the lean 16-column shape — no
        # result_full join column (the regression that started the rewrite).
        from src.server.database.provenance import _INSERT_COLUMNS

        assert "result_full" not in _INSERT_COLUMNS
        assert len(_INSERT_COLUMNS) == 16

    def test_no_result_full_helpers_on_module(self):
        # The join helpers/const that populated result_full are gone too.
        import src.server.database.provenance as prov

        assert not hasattr(prov, "_RESULT_FULL_MAX_CHARS")
        assert not hasattr(prov, "_content_text")
        assert not hasattr(prov, "_tool_result_contents")


class TestBodyCapStaysInSyncWithAgentSide:
    def test_max_bytes_matches_canonical_agent_constant(self):
        # RESULT_BODY_MAX_BYTES is re-declared in this module (not imported) to
        # keep the server side out of the agent import graph; both copies MUST
        # hold the same value — the inline/spill boundary IS the in-sandbox
        # transport cap. Pin the invariant so editing one can't silently desync.
        from ptc_agent.agent.provenance.types import (
            RESULT_BODY_MAX_BYTES as AGENT_MAX_BYTES,
        )

        assert RESULT_BODY_MAX_BYTES == AGENT_MAX_BYTES
