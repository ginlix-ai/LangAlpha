"""Contracts for the blob backfill operator script.

Only the decisions that decide whether a rollback is byte-exact are pinned
here: which inline column the bytes go back into, and what the reverse pass
refuses to write. The upload path is exercised against a real bucket, not
mocked.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest


def _load_backfill():
    """Import the script by path — it lives outside any package."""
    name = "_backfill_under_test"
    path = (
        Path(__file__).resolve().parents[4]
        / "scripts"
        / "ops"
        / "backfill_workspace_file_blobs.py"
    )
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    # Registered before exec: the script's dataclasses have string annotations
    # (`from __future__ import annotations`), which dataclasses resolves via
    # sys.modules[cls.__module__].
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


backfill = _load_backfill()


def test_binary_rows_restore_to_bytea():
    assert backfill._inline_columns(b"\x89PNG\x00", is_binary=True) == (None, b"\x89PNG\x00")


def test_clean_text_restores_to_text():
    assert backfill._inline_columns("hello 股票".encode(), is_binary=False) == (
        "hello 股票",
        None,
    )


def test_nul_bearing_text_restores_to_bytea_byte_exact():
    """The rollback path must not write a lossy copy.

    ``_detect_is_binary`` only scans the first 64KiB for NUL, so a large UTF-8
    file with a NUL past that is stored as text. Stripping the NUL to satisfy
    Postgres would leave ``content_hash`` describing bytes the row no longer
    holds — exactly the state the forward pass refuses to touch, stranding the
    row inline forever with its real bytes only in an orphaned blob.
    """
    data = b"a" * 70_000 + b"\x00" + b"b" * 10
    text, binary = backfill._inline_columns(data, is_binary=False)
    assert text is None
    assert binary == data


def test_empty_file_restores_to_text_not_dropped():
    assert backfill._inline_columns(b"", is_binary=False) == ("", None)


@pytest.mark.parametrize("sha", ["", "not-a-digest", "../../etc/passwd"])
def test_uploads_go_through_the_shared_key_guard(sha):
    """The script builds keys with the same validated helper as the store."""
    from src.server.database.blob_keys import BlobError, blob_key

    assert backfill.blob_key is blob_key
    with pytest.raises(BlobError):
        blob_key("user-1", sha)


def test_invalid_utf8_in_a_text_row_restores_to_bytea_byte_exact():
    """``is_binary`` is set by a 64KiB NUL scan, so a text-classified file can
    still hold invalid UTF-8 further in. Decoding it with ``errors="replace"``
    would write U+FFFD where the original bytes were and leave ``content_hash``
    describing bytes the row no longer holds."""
    data = b"a" * 70_000 + b"\xff\xfe" + b"b" * 10
    assert backfill._inline_columns(data, is_binary=False) == (None, data)


# --- the reverse pass verifies before it NULLs the pointer -------------------


class _Conn:
    """Records statements; ``rowcount`` decides whether the reverse CAS matched."""

    def __init__(self, rowcount: int = 1):
        self.statements: list[tuple] = []
        self.rowcount = rowcount

    def cursor(self, row_factory=None):
        conn = self

        class _Cur:
            rowcount = conn.rowcount

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def execute(self, sql, params=None):
                conn.statements.append((" ".join(sql.split()), params))

            async def fetchone(self):
                return (0,)  # _count_blob_backed: nothing left blob-backed

        return _Cur()


def _row(**over):
    data = over.pop("_data", b"hello")
    sha = hashlib.sha256(data).hexdigest()
    row = {
        "workspace_file_id": uuid.uuid4(),
        "workspace_id": "ws-1",
        "user_id": "user-1",
        "file_path": "a.txt",
        "content_hash": sha,
        "content_text": None,
        "content_binary": None,
        "blob_sha256": sha,
        "pack_sha256": None,
        "pack_offset": None,
        "file_size": len(data),
        "is_binary": False,
    }
    row.update(over)
    return row


async def _run_reverse(row, *, blob_bytes=None, range_bytes=None, rowcount=1):
    """Drive ``_reverse`` over one row. Returns ``(exit_code, statements)``."""
    conn = _Conn(rowcount)
    pages = iter([[str(row["workspace_file_id"])], []])

    async def _next_ids(*a, **k):
        return next(pages)

    async def _load_row(*a, **k):
        return row

    with (
        patch.object(backfill, "_next_ids", _next_ids),
        patch.object(backfill, "_load_row", _load_row),
        patch.object(backfill, "get_bytes", lambda key: blob_bytes),
        patch.object(backfill, "get_bytes_range", lambda key, off, length: range_bytes),
    ):
        code = await backfill._reverse(conn, True, 10, None)
    updates = [st for st in conn.statements if st[0].startswith("UPDATE workspace_files")]
    return code, updates


@pytest.mark.asyncio
async def test_reverse_cas_pins_every_column_the_read_observed():
    """Two packs with different member boundaries can share a digest, so the
    pointer columns alone do not identify the row that was read: a re-pointed
    row would take a slice cut for its predecessor."""
    data = b"hello"
    row = _row(_data=data)
    code, updates = await _run_reverse(row, blob_bytes=data)

    assert code == 0
    assert len(updates) == 1
    sql, params = updates[0]
    for column in ("blob_sha256", "pack_sha256", "content_hash", "pack_offset", "file_size"):
        assert f"AND {column} IS NOT DISTINCT FROM %s" in sql
    assert params[-5:] == (
        row["blob_sha256"],
        None,
        row["content_hash"],
        None,
        row["file_size"],
    )


@pytest.mark.asyncio
async def test_reverse_skips_a_row_whose_object_is_the_wrong_length():
    """The write NULLs the only pointer to the good bytes, so a short read has
    to be reported rather than restored."""
    code, updates = await _run_reverse(_row(file_size=99), blob_bytes=b"hello")
    assert updates == []
    assert code == 1


@pytest.mark.asyncio
async def test_reverse_verifies_length_even_with_no_stored_hash():
    """A packed row's expected digest is ``content_hash``, which can be NULL.
    Length is the check that still applies, and skipping it would restore a
    truncated member."""
    row = _row(
        blob_sha256=None,
        pack_sha256=hashlib.sha256(b"chunk").hexdigest(),
        pack_offset=0,
        content_hash=None,
        file_size=5,
    )
    code, updates = await _run_reverse(row, range_bytes=b"hel")
    assert updates == []
    assert code == 1


async def _run_forward(row, *, include_legacy_owners=False):
    """Dry-run ``_forward`` over one row. Returns the counts the summary logs."""
    conn = _Conn()
    pages = iter([[str(row["workspace_file_id"])], []])

    async def _next_ids(*a, **k):
        return next(pages)

    async def _load_row(*a, **k):
        return row

    with (
        patch.object(backfill, "_next_ids", _next_ids),
        patch.object(backfill, "_load_row", _load_row),
        patch.object(backfill.logger, "info") as info,
    ):
        await backfill._forward(conn, False, 10, None, include_legacy_owners)
    summary = info.call_args.args
    return dict(zip(("moved", "bytes", "lossy", "raced", "empty", "legacy", "failed"), summary[2:]))


@pytest.mark.asyncio
async def test_a_legacy_owner_is_left_inline_by_default():
    """A non-UUID owner is re-keyed by its first platform login; objects
    under the old id would not follow it."""
    row = _row(user_id="legacy-user", blob_sha256=None, content_text="hello")
    counts = await _run_forward(row)
    assert counts["legacy"] == 1 and counts["moved"] == 0


@pytest.mark.asyncio
async def test_include_legacy_owners_moves_the_row():
    row = _row(user_id="legacy-user", blob_sha256=None, content_text="hello")
    counts = await _run_forward(row, include_legacy_owners=True)
    assert counts["legacy"] == 0 and counts["moved"] == 1


@pytest.mark.asyncio
async def test_a_uuid_owner_moves_without_the_flag():
    row = _row(user_id=str(uuid.uuid4()), blob_sha256=None, content_text="hello")
    counts = await _run_forward(row)
    assert counts["legacy"] == 0 and counts["moved"] == 1
