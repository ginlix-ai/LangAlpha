"""The blob garbage-collection protocol, exercised against real Postgres.

The protocol's guarantees are about lock ordering between a writer and the
reap, which no mocked cursor can express. Object storage is a dict here: the
race is in the database, and the store only has to be observable.

Every interleaving below was first run live against a bucket
(``gcprobe.py`` in the session scratchpad); these pin the decision table.
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest
import pytest_asyncio
from psycopg_pool import AsyncConnectionPool

from src.server.database import workspace_file_blobs as B

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Store:
    """Just enough of the storage facade for the registry to be observable."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.refuse_delete = False

    def upload(self, key, data, content_type=None, max_size=None) -> bool:
        self.objects[key] = data
        return True

    def get(self, key):
        return self.objects.get(key)

    def delete(self, key) -> bool:
        if self.refuse_delete:
            return False
        self.objects.pop(key, None)
        return True


@pytest_asyncio.fixture
async def gc(test_db_uri, test_db_pool, seed_workspace):
    """A tuple-row pool (what production hands the module) patched in, plus a
    fake store. Yields ``(store, run_sql, workspace_id)``."""
    pool = AsyncConnectionPool(
        conninfo=test_db_uri,
        min_size=1,
        max_size=4,
        kwargs={"autocommit": True, "prepare_threshold": 0},
        open=False,
    )
    await pool.open()
    await pool.wait()

    @asynccontextmanager
    async def _gdc(conn=None):
        if conn is not None:
            yield conn
            return
        async with pool.connection() as owned:
            yield owned

    async def run(sql, params=()):
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(sql, params)
            return await cur.fetchall() if cur.description else None

    store = _Store()
    with (
        patch.object(B, "get_db_connection", _gdc),
        patch.object(B, "_storage_upload_bytes", store.upload),
        patch.object(B, "_storage_get_bytes", store.get),
        patch.object(B, "_storage_delete_object", store.delete),
    ):
        try:
            yield store, run, seed_workspace["workspace_id"], seed_workspace["user_id"]
        finally:
            await run("DELETE FROM workspace_files WHERE workspace_id = %s", (seed_workspace["workspace_id"],))
            await run("DELETE FROM workspace_file_blobs")
            await pool.close()


def _blob(tag: str) -> tuple[str, bytes]:
    data = f"gc {tag} {uuid.uuid4()}".encode()
    return hashlib.sha256(data).hexdigest(), data


async def _state(run, sha):
    rows = await run("SELECT condemned_at IS NOT NULL FROM workspace_file_blobs WHERE sha256 = %s", (sha,))
    return None if not rows else ("condemned" if rows[0][0] else "live")


async def _age_out(run, sha, *, days=8):
    await run(
        "UPDATE workspace_file_blobs SET last_referenced_at = NOW() - make_interval(days => %s) WHERE sha256 = %s",
        (days, sha),
    )


async def _condemned(run, sha):
    await _age_out(run, sha)
    await B.condemn_orphan_blobs()
    assert await _state(run, sha) == "condemned"


async def test_lifecycle_condemn_then_reap(gc):
    store, run, _, uid = gc
    sha, data = _blob("a")
    await B.store_blob(uid, sha, data)
    assert await _state(run, sha) == "live" and store.get(B.blob_key(uid, sha)) == data

    await _age_out(run, sha)
    assert await B.condemn_orphan_blobs() == 1
    assert await _state(run, sha) == "condemned"
    # Still inside the second grace: nothing happens.
    assert await B.reap_condemned_blobs(condemned_grace_hours=24) == (0, 0)
    assert await B.reap_condemned_blobs(condemned_grace_hours=0) == (1, 0)
    assert await _state(run, sha) is None and store.get(B.blob_key(uid, sha)) is None


async def test_touch_shields_from_condemnation(gc):
    store, run, _, uid = gc
    sha, data = _blob("b")
    await B.store_blob(uid, sha, data)
    await _age_out(run, sha)
    assert await B.registered_blobs(uid, [sha]) == {sha}
    assert await B.condemn_orphan_blobs() == 0
    assert await _state(run, sha) == "live"


async def test_manifest_reference_shields_from_condemnation(gc):
    store, run, ws, uid = gc
    sha, data = _blob("c")
    await B.store_blob(uid, sha, data)
    await _age_out(run, sha)
    await run(
        "INSERT INTO workspace_files (workspace_id, file_path, file_name, file_size, content_hash, blob_sha256) "
        "VALUES (%s, 'c.txt', 'c.txt', %s, %s, %s)",
        (ws, len(data), sha, sha),
    )
    assert await B.condemn_orphan_blobs() == 0
    assert await _state(run, sha) == "live"


async def test_pack_membership_shields_the_chunk_from_condemnation(gc):
    """A chunk is referenced through pack_sha256, never blob_sha256."""
    store, run, ws, uid = gc
    sha, data = _blob("chunk")
    await B.store_blob(uid, sha, data)
    await _age_out(run, sha)
    await run(
        "INSERT INTO workspace_files (workspace_id, file_path, file_name, file_size, content_hash, pack_sha256, pack_offset) "
        "VALUES (%s, 'm.txt', 'm.txt', 2, repeat('0', 64), %s, 0)",
        (ws, sha),
    )
    assert await B.condemn_orphan_blobs() == 0
    assert await _state(run, sha) == "live"
    await run("DELETE FROM workspace_files WHERE workspace_id = %s AND file_path = 'm.txt'", (ws,))
    assert await B.condemn_orphan_blobs() == 1


async def test_condemned_row_is_invisible_claimed_and_revived(gc):
    store, run, _, uid = gc
    sha, data = _blob("d")
    await B.store_blob(uid, sha, data)
    await _condemned(run, sha)
    before = (await run("SELECT condemned_at FROM workspace_file_blobs WHERE sha256 = %s", (sha,)))[0][0]

    # Not registered as far as a writer is concerned, but the check claims it.
    assert await B.registered_blobs(uid, [sha]) == set()
    after = (await run("SELECT condemned_at FROM workspace_file_blobs WHERE sha256 = %s", (sha,)))[0][0]
    assert after > before

    await B.register_blobs(uid, [(sha, len(data))])
    assert await _state(run, sha) == "live"
    assert await B.reap_condemned_blobs(condemned_grace_hours=0) == (0, 0)
    assert store.get(B.blob_key(uid, sha)) == data


async def test_writer_arriving_under_the_reap_lock_lands_live_with_its_object(gc):
    """The reap holds the row lock across the object delete. A writer's claim
    queues behind it, then finds no row, uploads, and inserts fresh."""
    store, run, _, uid = gc
    sha, data = _blob("e1")
    await B.store_blob(uid, sha, data)
    await _condemned(run, sha)

    async with B.get_db_connection() as reap:
        async with reap.transaction():
            async with reap.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM workspace_file_blobs WHERE sha256 = %s AND condemned_at < NOW() FOR UPDATE",
                    (sha,),
                )
                assert await cur.fetchone()
            writer = asyncio.create_task(B.store_blob(uid, sha, data))
            await asyncio.sleep(0.5)
            assert not writer.done(), "the writer's claim must block behind the row lock"
            store.delete(B.blob_key(uid, sha))
            async with reap.cursor() as cur:
                await cur.execute("DELETE FROM workspace_file_blobs WHERE user_id = %s AND sha256 = %s", (uid, sha))
        await writer

    assert await _state(run, sha) == "live"
    assert store.get(B.blob_key(uid, sha)) == data


async def test_claim_that_lands_first_takes_the_row_out_of_the_reap(gc):
    store, run, _, uid = gc
    sha, data = _blob("e2")
    await B.store_blob(uid, sha, data)
    await _condemned(run, sha)
    await run("UPDATE workspace_file_blobs SET condemned_at = NOW() - interval '2 hours' WHERE sha256 = %s", (sha,))

    await B.registered_blobs(uid, [sha])  # the sync path's claim, before its upload
    assert await B.reap_condemned_blobs(condemned_grace_hours=1) == (0, 0)
    assert await _state(run, sha) == "condemned"
    assert store.get(B.blob_key(uid, sha)) == data


async def test_store_failure_leaves_the_row_condemned_for_the_next_cycle(gc):
    store, run, _, uid = gc
    sha, data = _blob("f")
    await B.store_blob(uid, sha, data)
    await _condemned(run, sha)

    store.refuse_delete = True
    assert await B.reap_condemned_blobs(condemned_grace_hours=0) == (0, 1)
    assert await _state(run, sha) == "condemned" and store.get(B.blob_key(uid, sha)) == data

    store.refuse_delete = False
    assert await B.reap_condemned_blobs(condemned_grace_hours=0) == (1, 0)
    assert await _state(run, sha) is None and store.get(B.blob_key(uid, sha)) is None


async def test_sweep_is_single_flight_across_processes(gc):
    store, run, _, uid = gc
    async with B.get_db_connection() as other, other.cursor() as cur:
        await cur.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (B._GC_LOCK_KEY,))
        assert await B.sweep_blob_garbage() is None
        await cur.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (B._GC_LOCK_KEY,))
    assert await B.sweep_blob_garbage() == {"condemned": 0, "deleted": 0, "failed": 0}
