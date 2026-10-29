"""Saves of one store file from two server workers, against a real Postgres.

Each worker orders its own writers with an in-process lock the other never
sees, so here each save gets a lock of its own, as two workers' would, and
only the advisory lock ``namespace_write_lock`` takes on the app database
stands between them.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator

import pytest
from langgraph.store.postgres import AsyncPostgresStore
from psycopg_pool import AsyncConnectionPool

from ptc_agent.agent.backends import StoreBackend, langgraph_store
from src.server.services.livefs.routes import LivefsError, adapt

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

ROOT = "/work/.agents/user/memory/"
PATH = ROOT + "notes.md"


class _StubSandbox:
    """The backend keeps files in the store, never in the sandbox."""

    def normalize_path(self, p): return p
    def virtualize_path(self, p): return p
    def validate_path(self, p): return True
    @property
    def filesystem_config(self): return None


class _ReadsTogether(AsyncPostgresStore):
    """Once racing, holds each read open until the other save has read too,
    or half a second, so with nothing between them both read one version."""

    racing = False

    async def aget(self, *args, **kwargs):
        item = await super().aget(*args, **kwargs)
        if self.racing:
            self.reads += 1
            if self.reads == 2:
                self.both_read.set()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.both_read.wait(), 0.5)
        return item

    def race(self) -> None:
        self.racing, self.reads, self.both_read = True, 0, asyncio.Event()


@contextlib.asynccontextmanager
async def _store(test_db_uri: str) -> AsyncIterator[_ReadsTogether]:
    """A store on a pool of its own, as the server's shares the checkpointer's
    pool rather than the app's. Built on the test's loop, which its batching
    task is bound to."""
    pool = AsyncConnectionPool(
        conninfo=test_db_uri,
        min_size=1,
        max_size=4,
        kwargs={"prepare_threshold": 0},
        open=False,
    )
    await pool.open()
    try:
        yield _ReadsTogether(conn=pool)
    finally:
        await pool.close()


@pytest.fixture
def namespace(monkeypatch) -> tuple[str, ...]:
    monkeypatch.setattr(langgraph_store, "lock_for_namespace", lambda _namespace: asyncio.Lock())
    return (f"store-lock-{uuid.uuid4().hex[:12]}", "memory")


def _backend(store: AsyncPostgresStore, namespace: tuple[str, ...]) -> StoreBackend:
    return StoreBackend(
        store=store,
        namespace_factory=lambda: namespace,
        root_prefix=ROOT,
        sandbox_backend=_StubSandbox(),  # type: ignore[arg-type]
    )


async def test_two_workers_saves_over_one_version_land_once(
    test_db_pool, patched_get_db_connection, test_db_uri, namespace
):
    async with _store(test_db_uri) as store:
        route = adapt(_backend(store, namespace))
        try:
            assert await _backend(store, namespace).awrite_text(PATH, "base")
            _, version = await route.read(PATH)

            store.race()
            results = await asyncio.gather(
                route.write(PATH, "from A", version),
                route.write(PATH, "from B", version),
                return_exceptions=True,
            )

            saved = [r for r in results if not isinstance(r, BaseException)]
            refused = [r for r in results if isinstance(r, LivefsError)]
            assert len(saved) == len(refused) == 1, results
            assert (refused[0].status, refused[0].code) == (412, "changed")
            store.racing = False
            content, version = await route.read(PATH)
            assert content in ("from A", "from B")
            assert version == saved[0].version
        finally:
            await store.adelete(namespace, "notes.md")


async def test_a_save_waiting_past_the_limit_is_refused_as_unavailable(
    test_db_pool, patched_get_db_connection, test_db_uri, namespace, monkeypatch
):
    """A holder stuck past the limit fails the save rather than hanging it,
    and the failure reads as the store timing out: not saved, retry."""
    monkeypatch.setattr(langgraph_store, "_WRITE_LOCK_WAIT", "100ms")
    async with _store(test_db_uri) as store, test_db_pool.connection() as holder:
        route = adapt(_backend(store, namespace))
        async with holder.transaction():
            await holder.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                ("store-write:" + "/".join(namespace),),
            )
            with pytest.raises(LivefsError) as refused:
                await route.write(PATH, "new", None)

        assert (refused.value.status, refused.value.code) == (503, "unavailable")
        assert await route.read(PATH) is None
