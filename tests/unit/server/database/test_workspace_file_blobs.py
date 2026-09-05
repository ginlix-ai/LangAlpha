"""Contracts for the content-addressed workspace file blob store.

Pins the invariants that are easy to regress silently: the object key must be
derived only from a real digest, ``b""`` is a legitimate blob, and a fetch that
comes back wrong must raise rather than hand a caller the wrong file.
"""

from __future__ import annotations

import asyncio
import hashlib
import threading
from unittest.mock import patch

import pytest

from src.server.database import workspace_file_blobs as blobs
from src.server.database.blob_keys import BLOB_KEY_PREFIX, MAX_BLOB_BYTES
from src.server.database.workspace_file_blobs import (
    BlobError,
    BlobFetchError,
    BlobUploadError,
    blob_key,
    fetch_blob,
    fetch_blob_range,
    registered_blobs,
    store_blob,
)

EMPTY_SHA = hashlib.sha256(b"").hexdigest()
HELLO = b"hello blob"
HELLO_SHA = hashlib.sha256(HELLO).hexdigest()
USER = "user-a"


class _FakeConn:
    """Minimal async connection stub; records executed statements."""

    def __init__(self, fail: bool = False):
        self.fail = fail
        self.statements: list[tuple] = []

    def cursor(self):
        conn = self

        class _Cur:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def execute(self, sql, params=None):
                if conn.fail:
                    raise RuntimeError("db down")
                conn.statements.append((sql, params))

        return _Cur()


class _RowsConn(_FakeConn):
    """A connection that also answers with a scripted result set."""

    def __init__(self, rows: list[tuple]):
        super().__init__()
        self.rows = rows

    def cursor(self):
        conn = self

        class _Cur:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def execute(self, sql, params=None):
                conn.statements.append((sql, params))

            async def fetchall(self):
                return conn.rows

        return _Cur()


class _ReapConn:
    """Enough psycopg surface for ``_reap_one``: a transaction whose exit is
    recorded in ``events``, and a cursor answering the reap's two probes."""

    def __init__(self, events: list[str]):
        self.events = events

    def transaction(self):
        conn = self

        class _Tx:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                conn.events.append("transaction end")
                return False

        return _Tx()

    def cursor(self):
        conn = self

        class _Cur:
            sql = ""

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def execute(self, sql, params=None):
                self.sql = " ".join(sql.split())
                if self.sql.startswith("DELETE"):
                    conn.events.append("row deleted")

            async def fetchone(self):
                # The FOR UPDATE probe finds the condemned row; the reference
                # re-check finds nothing, so the reap proceeds to the delete.
                return (1,) if "FOR UPDATE" in self.sql else None

        return _Cur()


def _squash(sql: str) -> str:
    """Collapse SQL whitespace so an embedded statement can be matched."""
    return " ".join(sql.split())


def _conn_ctx(conn):
    class _Ctx:
        async def __aenter__(self):
            return conn

        async def __aexit__(self, *exc):
            return False

    return lambda: _Ctx()


@pytest.mark.parametrize(
    "bad",
    [
        "",
        None,
        "not-a-digest",
        "../../etc/passwd",
        HELLO_SHA.upper(),  # uppercase hex is not the canonical digest form
        HELLO_SHA[:-1],
        HELLO_SHA + "a",
        f"{HELLO_SHA}/../evil",
    ],
)
def test_blob_key_refuses_non_digests(bad):
    """The key is derived from the digest, so a non-digest must never form one."""
    with pytest.raises(BlobError):
        blob_key(USER, bad)


def test_blob_key_shape():
    assert blob_key(USER, HELLO_SHA) == f"{BLOB_KEY_PREFIX}{USER}/{HELLO_SHA}"


@pytest.mark.parametrize("bad", ["", None, "../other", "a/b", "user a", "-x", "x" * 256])
def test_blob_key_refuses_malformed_user_ids(bad):
    """The user segment is a path component; it must never carry a separator
    or an empty value that would land the object in another namespace."""
    with pytest.raises(BlobError):
        blob_key(bad, HELLO_SHA)


def test_blob_keys_module_stays_import_clean():
    """``scripts/ops/`` shares these constants by importing this module.

    It can import ``blob_keys`` only because that module pulls in nothing but
    ``re``. An import of the app's config (directly or transitively) would fire
    ``load_dotenv()`` and silently retarget a mutating operator script at
    whatever ``.env`` is on disk.
    """
    import ast
    from pathlib import Path

    import src.server.database.blob_keys as blob_keys

    tree = ast.parse(Path(blob_keys.__file__).read_text())
    imported = {
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert imported <= {"re", "__future__"}


@pytest.mark.asyncio
async def test_store_blob_uploads_then_registers():
    conn = _FakeConn()
    with (
        patch.object(blobs, "_storage_upload_bytes", return_value=True) as up,
        patch.object(blobs, "get_db_connection", _conn_ctx(conn)),
    ):
        await store_blob(USER, HELLO_SHA, HELLO)

    # Upload carries the explicit cap; the shared 10MB facade default would
    # otherwise reject every file between 10MB and MAX_FILE_SIZE.
    assert up.call_args.args[0] == blob_key(USER, HELLO_SHA)
    assert up.call_args.kwargs["max_size"] == MAX_BLOB_BYTES
    # Claim first, upload, then the reviving upsert: a live row always has an
    # object behind it, and a concurrent reap cannot slip between the two.
    assert len(conn.statements) == 2
    claim, register = conn.statements
    assert "condemned_at IS NOT NULL" in claim[0] and claim[1] == (USER, [HELLO_SHA])
    assert "ON CONFLICT (user_id, sha256) DO UPDATE" in register[0] and "condemned_at = NULL" in register[0]
    assert register[1] == (USER, HELLO_SHA, len(HELLO))


@pytest.mark.asyncio
async def test_store_blob_registers_empty_blob():
    """sha256(b"") is a real blob — an empty file must round-trip, not vanish."""
    conn = _FakeConn()
    with (
        patch.object(blobs, "_storage_upload_bytes", return_value=True),
        patch.object(blobs, "get_db_connection", _conn_ctx(conn)),
    ):
        await store_blob(USER, EMPTY_SHA, b"")
    assert conn.statements[-1][1] == (USER, EMPTY_SHA, 0)


@pytest.mark.asyncio
async def test_store_blob_raises_when_upload_rejected():
    """The facade returns False rather than raising; this is where that becomes
    an error the sync path can count."""
    with (
        patch.object(blobs, "_storage_upload_bytes", return_value=False),
        patch.object(blobs, "get_db_connection", _conn_ctx(_FakeConn())),
    ):
        with pytest.raises(BlobUploadError):
            await store_blob(USER, HELLO_SHA, HELLO)


@pytest.mark.asyncio
async def test_store_blob_raises_when_registry_insert_fails():
    conn = _FakeConn(fail=True)
    with (
        patch.object(blobs, "_storage_upload_bytes", return_value=True),
        patch.object(blobs, "get_db_connection", _conn_ctx(conn)),
    ):
        with pytest.raises(BlobUploadError):
            await store_blob(USER, HELLO_SHA, HELLO)


@pytest.mark.asyncio
async def test_store_blob_never_deletes_on_failure():
    """An orphan object is cheap; deleting a shared content-addressed key another
    writer already committed a manifest row against is not."""
    conn = _FakeConn(fail=True)
    with (
        patch.object(blobs, "_storage_upload_bytes", return_value=True),
        patch.object(blobs, "_storage_delete_object") as delete,
        patch.object(blobs, "get_db_connection", _conn_ctx(conn)),
    ):
        with pytest.raises(BlobUploadError):
            await store_blob(USER, HELLO_SHA, HELLO)
    delete.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_blob_round_trips():
    with patch.object(blobs, "_storage_get_bytes", return_value=HELLO):
        assert await fetch_blob(USER, HELLO_SHA) == HELLO


@pytest.mark.asyncio
async def test_fetch_blob_returns_empty_bytes_not_an_error():
    """`if data is None`, never truthiness — b"" is falsy but valid."""
    with patch.object(blobs, "_storage_get_bytes", return_value=b""):
        assert await fetch_blob(USER, EMPTY_SHA) == b""


@pytest.mark.asyncio
async def test_fetch_blob_raises_on_missing_object():
    with patch.object(blobs, "_storage_get_bytes", return_value=None):
        with pytest.raises(BlobFetchError):
            await fetch_blob(USER, HELLO_SHA)


@pytest.mark.asyncio
async def test_fetch_blob_raises_on_content_mismatch():
    """Silent corruption would restore the wrong file; make it loud instead."""
    with patch.object(blobs, "_storage_get_bytes", return_value=b"tampered"):
        with pytest.raises(BlobFetchError):
            await fetch_blob(USER, HELLO_SHA)


# --- ranged reads for pack members ------------------------------------------

MEMBER = b"lo b"
MEMBER_SHA = hashlib.sha256(MEMBER).hexdigest()


@pytest.mark.asyncio
async def test_fetch_blob_range_reads_and_verifies_the_member():
    with patch.object(blobs, "_storage_get_bytes_range", return_value=MEMBER) as get:
        assert await fetch_blob_range(USER, HELLO_SHA, 3, 4, expected_sha256=MEMBER_SHA) == MEMBER
    get.assert_called_once_with(blob_key(USER, HELLO_SHA), 3, 4)


@pytest.mark.asyncio
async def test_fetch_blob_range_raises_on_missing_short_or_wrong_bytes():
    """A wrong offset or a mis-keyed chunk must be as loud as a corrupt object."""
    with patch.object(blobs, "_storage_get_bytes_range", return_value=None):
        with pytest.raises(BlobFetchError):
            await fetch_blob_range(USER, HELLO_SHA, 3, 4)
    with patch.object(blobs, "_storage_get_bytes_range", return_value=b"lo"):
        with pytest.raises(BlobFetchError):
            await fetch_blob_range(USER, HELLO_SHA, 3, 4)
    with patch.object(blobs, "_storage_get_bytes_range", return_value=b"xxxx"):
        with pytest.raises(BlobFetchError):
            await fetch_blob_range(USER, HELLO_SHA, 3, 4, expected_sha256=MEMBER_SHA)


# --- registry dedup is scoped to what the caller already has -----------------


@pytest.mark.asyncio
async def test_registered_blobs_reports_live_rows_under_the_user():
    """A hit is a live registry row under the caller's own user: the registry
    is keyed per user, so a hit means the user already holds those bytes."""
    conn = _RowsConn([(HELLO_SHA, True), (EMPTY_SHA, False)])
    with patch.object(blobs, "get_db_connection", _conn_ctx(conn)):
        assert await registered_blobs(USER, [HELLO_SHA, EMPTY_SHA]) == {HELLO_SHA}
    sql, params = conn.statements[0]
    assert "last_referenced_at = NOW()" in sql and "user_id = %s" in sql
    assert params[0] == USER
    assert sorted(params[1]) == sorted([HELLO_SHA, EMPTY_SHA])


@pytest.mark.asyncio
async def test_registered_blobs_touches_every_digest_it_was_asked_about():
    """An unreported digest is one the caller is about to upload, so its row
    still needs the touch that keeps the sweep off it."""
    conn = _RowsConn([])
    with patch.object(blobs, "get_db_connection", _conn_ctx(conn)):
        assert await registered_blobs(USER, [HELLO_SHA]) == set()
    sql, params = conn.statements[0]
    assert _squash(blobs._TOUCH_SQL) in _squash(sql)
    assert params == (USER, [HELLO_SHA])


class _PerUserConn(_FakeConn):
    """Answers only for the user the rows were registered under."""

    def __init__(self, owner: str, rows: list[tuple]):
        super().__init__()
        self.owner = owner
        self.rows = rows

    def cursor(self):
        conn = self

        class _Cur:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def execute(self, sql, params=None):
                conn.statements.append((sql, params))
                conn.matched = params[0] == conn.owner

            async def fetchall(self):
                return conn.rows if conn.matched else []

        return _Cur()


@pytest.mark.asyncio
async def test_digest_registered_under_one_user_is_a_miss_for_another():
    """A digest the sandbox merely names must not become a pointer to bytes the
    user never had, and the answer must not reveal who else holds them."""
    conn = _PerUserConn("user-a", [(HELLO_SHA, True)])
    with patch.object(blobs, "get_db_connection", _conn_ctx(conn)):
        assert await registered_blobs("user-a", [HELLO_SHA]) == {HELLO_SHA}
        assert await registered_blobs("user-b", [HELLO_SHA]) == set()
    assert [p[0] for _, p in conn.statements] == ["user-a", "user-b"]


# --- the reap's object delete outlives a cancellation ------------------------


@pytest.mark.asyncio
async def test_reap_holds_the_row_lock_until_a_cancelled_delete_settles():
    """Shutdown must not roll the transaction back with the delete in flight.

    The lock is the only thing stopping a writer from claiming the digest,
    uploading, and reviving it; released early, the still-running thread would
    then delete that writer's fresh object.
    """
    events: list[str] = []
    started = threading.Event()
    release = threading.Event()

    def _delete(key):
        started.set()
        release.wait(5)
        events.append("object deleted")
        return True

    conn = _ReapConn(events)
    with patch.object(blobs, "_storage_delete_object", _delete):
        task = asyncio.create_task(blobs._reap_one(conn, USER, HELLO_SHA, 24))
        while not started.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0.05)
        assert not task.done(), "the cancel must not abandon the running delete"
        assert events == [], "the transaction must still be open"
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert events == ["object deleted", "transaction end"]
