"""Contracts for the per-workspace fence around ``sync_to_db``.

A sync decides what to write from a manifest read taken before the sandbox
scan, then writes unconditionally minutes later. With ``--workers N`` two syncs
of one workspace can be in flight on different processes, and without a fence
the older pass's upsert lands last and reinstates rows the newer pass had
already superseded. Postgres holds the fence because it is the only thing all
the workers share.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from psycopg.errors import LockNotAvailable

from src.server.database import workspace_file as wf
from src.server.services.persistence import backup, restore
from src.server.services.persistence.transfer import ScanResult


class _Cursor:
    def __init__(self, executed: list, fail_on: str | None, unlock_delay: int = 0) -> None:
        self._executed = executed
        self._fail_on = fail_on
        self._unlock_delay = unlock_delay

    async def execute(self, sql: str, params=None) -> None:
        flat = " ".join(sql.split())
        self._executed.append((flat, params))
        if "pg_advisory_unlock" in flat:
            for _ in range(self._unlock_delay):
                await asyncio.sleep(0)
        if self._fail_on and self._fail_on in flat:
            if self._fail_on == "pg_advisory_lock":
                raise LockNotAvailable("canceling statement due to lock timeout")
            raise RuntimeError(f"statement refused: {self._fail_on}")

    async def fetchone(self):
        return (True,)


class _Conn:
    """Just enough of a psycopg connection for the lock's two statements."""

    def __init__(self) -> None:
        self.executed: list = []
        self.fail_on: str | None = None
        self.unlock_delay = 0
        self.closed = False

    @asynccontextmanager
    async def cursor(self, **_kw):
        yield _Cursor(self.executed, self.fail_on, self.unlock_delay)

    @asynccontextmanager
    async def transaction(self):
        self.executed.append(("BEGIN", None))
        try:
            yield
        except BaseException:
            self.executed.append(("ROLLBACK", None))
            raise
        self.executed.append(("COMMIT", None))

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def conn():
    c = _Conn()

    @asynccontextmanager
    async def _acquire(existing=None):
        yield c

    with patch.object(wf, "get_db_connection", _acquire):
        yield c


@pytest.mark.asyncio
async def test_the_lock_is_taken_and_released_on_one_session(conn):
    acquire = [
        "BEGIN",
        "SET LOCAL lock_timeout = '120s'",
        "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
        "COMMIT",
    ]
    async with wf.workspace_sync_lock("ws-1") as held:
        assert held is conn
        assert [sql for sql, _ in conn.executed] == acquire

    assert [sql for sql, _ in conn.executed] == acquire + [
        "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
    ]
    # Namespaced and keyed on the workspace, so two workspaces never contend.
    assert {params for _, params in conn.executed if params} == {
        ("WSFILES_SYNC:ws-1",)
    }


@pytest.mark.asyncio
async def test_a_wait_past_the_bound_raises_busy_without_touching_the_lock(conn):
    """A holder wedged on a dead sandbox would otherwise queue every later
    sync of the workspace behind it, each one pinning a pool slot forever."""
    conn.fail_on = "pg_advisory_lock"

    with pytest.raises(wf.WorkspaceSyncBusy, match="120s"):
        async with wf.workspace_sync_lock("ws-1"):
            raise AssertionError("body must not run")

    statements = [sql for sql, _ in conn.executed]
    assert statements[-1] == "ROLLBACK"
    assert not any("pg_advisory_unlock" in sql for sql in statements)
    assert conn.closed is False


@pytest.mark.asyncio
async def test_a_failed_sync_still_releases_the_lock(conn):
    """A session-level lock outlives its transaction, so nothing but the unlock
    frees it while the connection is alive and back in the pool."""
    with pytest.raises(RuntimeError):
        async with wf.workspace_sync_lock("ws-1"):
            raise RuntimeError("scan blew up")

    assert conn.executed[-1][0] == "SELECT pg_advisory_unlock(hashtextextended(%s, 0))"


@pytest.mark.asyncio
async def test_a_release_survives_cancellation_delivered_at_every_await(conn):
    """An AnyIO scope re-delivers the cancellation at each await, so a
    release that waits on the unlock once and then closes would lose the
    close to the second delivery and pool a session that still holds the
    lock."""
    conn.fail_on = "pg_advisory_unlock"
    conn.unlock_delay = 3
    started = asyncio.Event()

    async def sync():
        async with wf.workspace_sync_lock("ws-1"):
            started.set()
            await asyncio.sleep(60)

    task = asyncio.create_task(sync())
    await started.wait()
    for _ in range(6):
        task.cancel()
        await asyncio.sleep(0)
    with pytest.raises(asyncio.CancelledError):
        await task
    assert conn.executed[-1][0] == "SELECT pg_advisory_unlock(hashtextextended(%s, 0))"
    assert conn.closed is True


@pytest.mark.asyncio
async def test_a_cancelled_sync_still_releases_the_lock_on_the_live_session(conn):
    """A cancellation delivered at the unlock's own await would otherwise hand
    the pool a session that still holds the lock, and every later sync of the
    workspace on any worker would block on it forever."""
    with pytest.raises(asyncio.CancelledError):
        async with wf.workspace_sync_lock("ws-1"):
            raise asyncio.CancelledError()

    assert conn.executed[-1][0] == "SELECT pg_advisory_unlock(hashtextextended(%s, 0))"
    assert conn.closed is False


@pytest.mark.asyncio
async def test_an_unconfirmed_release_closes_the_session_so_the_lock_dies_with_it(conn):
    conn.fail_on = "pg_advisory_unlock"

    with pytest.raises(RuntimeError, match="statement refused"):
        async with wf.workspace_sync_lock("ws-1"):
            pass

    assert conn.closed is True


@pytest.mark.asyncio
async def test_sync_holds_the_lock_across_the_scan_and_the_writes():
    """The fence has to span the read, the scan and the write: serializing only
    the writes still lets the older pass's rows land last."""
    order: list[str] = []

    @asynccontextmanager
    async def _lock(workspace_id: str):
        order.append("lock")
        try:
            yield None
        finally:
            order.append("unlock")

    async def _scan(*_a, **_kw):
        order.append("scan")
        return ScanResult(entries=[], oversized=[], errors=[], hashed=0, reused=0)

    async def _delete(*_a, **_kw):
        order.append("delete")
        return 0

    sandbox = MagicMock()
    sandbox.working_dir = "/workspace"

    with (
        patch.object(backup, "workspace_sync_lock", _lock),
        patch.object(backup, "manifest_clock", new=AsyncMock(return_value=None)),
        patch.object(backup, "get_file_metadata_for_sync", new=AsyncMock(return_value={})),
        patch.object(backup, "files_restore_incomplete", new=AsyncMock(return_value=False)),
        patch.object(backup, "scan_workspace", new=_scan),
        patch.object(backup, "delete_removed_files", new=_delete),
    ):
        await backup.sync_to_db("ws-1", sandbox)

    assert order == ["lock", "scan", "delete", "unlock"]


@pytest.mark.asyncio
async def test_restore_holds_the_same_lock_across_the_flag_and_the_transfer():
    """A sync that scans while a restore is still filling the sandbox, then
    reads the completeness flag after the restore clears it, prunes every row
    its stale scan never saw arrive. Restore therefore takes the sync lock for
    its whole run, and its manifest read and the clearing of the flag ride
    that session; the flag is raised before the lock is requested, so a wait
    that times out has already recorded the sandbox as unfilled."""
    order: list[str] = []
    seen_conns: list = []

    @asynccontextmanager
    async def _lock(workspace_id: str):
        order.append("lock")
        try:
            yield "held-conn"
        finally:
            order.append("unlock")

    async def _flag(workspace_id, incomplete, *, conn=None, sandbox_id=None):
        order.append(f"flag={incomplete}")
        seen_conns.append(conn)
        return True

    async def _rows(workspace_id, *, include_content=False, all_kinds=False, conn=None):
        order.append("read")
        seen_conns.append(conn)
        return [{"file_path": "d", "kind": "dir", "permissions": "0755"}]

    async def _owner(workspace_id, conn=None):
        order.append("owner")
        seen_conns.append(conn)
        return "user-1"

    async def _pull(sandbox, items, **kw):
        order.append("pull")
        return {"d": {"status": "ok"}}

    sandbox = MagicMock()
    sandbox.working_dir = "/workspace"
    sandbox.aupload_file_bytes = AsyncMock(return_value=True)

    with (
        patch.object(restore, "workspace_sync_lock", _lock),
        patch.object(restore, "set_files_restore_incomplete", _flag),
        patch.object(restore, "get_files_for_workspace", _rows),
        patch.object(restore, "workspace_owner", _owner),
        patch.object(restore, "pull_direct", _pull),
    ):
        result = await restore.restore_to_sandbox("ws-1", sandbox)

    assert result == {"restored": 1, "errors": 0}
    assert order == ["flag=True", "lock", "read", "owner", "pull", "flag=False", "unlock"]
    assert seen_conns == [None] + ["held-conn"] * 3
