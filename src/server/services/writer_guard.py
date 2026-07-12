"""Writer guard v4 Phase 2 — the per-run pinned PG session (invariant I2).

One physical connection per run owns three things at once: the advisory-lock
fence (shared T(thread) + exclusive N(thread, root)), the run's START and
finalize SQL, and the run's own AsyncPostgresSaver. Because all three share
the one session, losing the session loses them together — a zombie writer
whose connection died can neither checkpoint nor finalize, structurally,
without any cooperative check. One asyncio mutex (installed as the saver's
own lock) serializes every operation on the pinned connection.

The guard activates only when the checkpointer is Postgres AND the app and
checkpoint tables live in the same database (advisory locks are
database-local). Otherwise every caller falls back to Phase-1 behavior:
global pooled saver, lifecycle SQL on the app pool, single worker only.
"""

import asyncio
import hashlib
import logging
from typing import Any, Callable, Optional

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from src.config.settings import get_writer_pool_max

logger = logging.getLogger(__name__)

__all__ = [
    "WriterGuard",
    "WriterGuardUnavailable",
    "GuardSessionLost",
    "advisory_key",
    "thread_key",
    "namespace_key",
    "ROOT_NS",
    "open_writer_pool",
    "close_writer_pool",
    "get_writer_pool",
    "guard_enabled",
    "same_database",
]

ROOT_NS = "root"

CHECKOUT_TIMEOUT = 2.0  # pool exhaustion -> bounded 503, never a queue
LOCK_RETRY_WINDOW = 2.0  # advisory-lock contention window (scanner holds are brief)
MONITOR_INTERVAL = 20.0
PING_TIMEOUT = 10.0


class WriterGuardUnavailable(Exception):
    """No pinned session available (budget exhausted or root lock contended).

    Maps to a retryable 503 at the HTTP boundary — bounded refusal, never an
    unfenced run.
    """


class GuardSessionLost(Exception):
    """The pinned connection died; this run may no longer write or finalize."""


# --------------------------------------------------------------------------
# Advisory key scheme — 64-bit, domain-separated (not 32-bit hashtext).
# --------------------------------------------------------------------------


def advisory_key(domain: str, *parts: str) -> int:
    """sha256("domain|part|part")[:8] as a signed bigint for pg advisory locks."""
    digest = hashlib.sha256("|".join((domain, *parts)).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


def thread_key(thread_id: str) -> int:
    """T(thread): shared by every root run; exclusive for thread mutations."""
    return advisory_key("T", thread_id)


def namespace_key(thread_id: str, namespace: str) -> int:
    """N(thread, ns): exclusive per checkpoint-namespace writer."""
    return advisory_key("N", thread_id, namespace)


# --------------------------------------------------------------------------
# Pinned-session pool
# --------------------------------------------------------------------------

_writer_pool: Optional[AsyncConnectionPool] = None


async def _configure_writer_conn(conn: AsyncConnection) -> None:
    conn.prepare_threshold = 0  # pooler (Supabase) compatibility


async def _reset_writer_conn(conn: AsyncConnection) -> None:
    """Backstop on putconn: session advisory locks survive rollback, so a
    guard that failed to unlock must never leak its locks into the next
    checkout."""
    await conn.execute("SELECT pg_advisory_unlock_all()")


def same_database(app_conninfo: str, checkpoint_conninfo: str) -> bool:
    """Advisory locks and the combined session are database-local: the fence
    only exists when app tables and checkpoint tables share one database."""
    from psycopg.conninfo import conninfo_to_dict

    try:
        a = conninfo_to_dict(app_conninfo)
        b = conninfo_to_dict(checkpoint_conninfo)
    except Exception:
        logger.warning("[WriterGuard] could not parse conninfo for same-DB check")
        return False
    keys = ("host", "port", "dbname")
    return all((a.get(k) or "") == (b.get(k) or "") for k in keys)


async def open_writer_pool(conninfo: str) -> AsyncConnectionPool:
    """Open the pinned-session pool (call once at startup, same-DB verified)."""
    global _writer_pool
    if _writer_pool is not None and not _writer_pool.closed:
        return _writer_pool
    max_size = get_writer_pool_max()
    _writer_pool = AsyncConnectionPool(
        conninfo=conninfo,
        min_size=1,
        max_size=max_size,
        configure=_configure_writer_conn,
        reset=_reset_writer_conn,
        check=AsyncConnectionPool.check_connection,
        open=False,
        kwargs={
            "autocommit": True,  # saver contract; lifecycle SQL uses explicit txns
            "connect_timeout": 10,
            "keepalives": 1,
            "keepalives_idle": 60,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        },
    )
    await _writer_pool.open()
    logger.info(f"[WriterGuard] pinned-session pool opened (budget={max_size})")
    return _writer_pool


async def close_writer_pool() -> None:
    global _writer_pool
    if _writer_pool is not None:
        try:
            await _writer_pool.close()
        finally:
            _writer_pool = None


def get_writer_pool() -> Optional[AsyncConnectionPool]:
    return _writer_pool


def guard_enabled() -> bool:
    return _writer_pool is not None and not _writer_pool.closed


# --------------------------------------------------------------------------
# WriterGuard
# --------------------------------------------------------------------------


class WriterGuard:
    """One run's pinned session: locks, lifecycle SQL, and saver on one conn.

    Lifecycle: ``acquire_root`` (shared T + exclusive N(root)) → execute →
    ``demote_to_tail`` at finalize if background-subagent writers survive the
    turn (drops N(root) so the next turn can start, keeps the session alive
    for the tail writers' saver) → ``release``. All idempotent; ``release``
    is safe on a dead connection.
    """

    def __init__(
        self,
        pool: AsyncConnectionPool,
        conn: AsyncConnection,
        thread_id: str,
        run_id: str,
    ) -> None:
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

        self._pool = pool
        self.conn = conn
        self.thread_id = thread_id
        self.run_id = run_id
        # THE one I/O mutex: installed as the saver's own lock so saver ops
        # and lifecycle SQL on this connection can never interleave.
        self.mutex = asyncio.Lock()
        self.saver = AsyncPostgresSaver(conn)
        self.saver.lock = self.mutex
        self.lost = False
        self._root_held = False
        self._shared_held = False
        self._released = False
        self._discard = False
        self._release_task: Optional[asyncio.Task] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._abort_cb: Optional[Callable[[], Any]] = None

    # ------------------------------------------------------------- acquire

    @classmethod
    async def acquire_root(cls, *, thread_id: str, run_id: str) -> "WriterGuard":
        """Checkout a pinned conn and take shared T + exclusive N(root).

        Raises WriterGuardUnavailable on budget exhaustion or lock contention
        (both bounded). The partial unique in_progress index is the admission
        authority; root-lock contention here means only a recovery scanner or
        a lock-lost predecessor is mid-release, so we retry briefly then 503.
        """
        pool = get_writer_pool()
        if pool is None:
            raise WriterGuardUnavailable("writer pool not open")
        try:
            conn = await pool.getconn(timeout=CHECKOUT_TIMEOUT)
        except PoolTimeout:
            raise WriterGuardUnavailable(
                f"pinned-session budget exhausted ({get_writer_pool_max()} held)"
            )
        guard: Optional["WriterGuard"] = None
        try:
            guard = cls(pool, conn, thread_id, run_id)
            await guard._take_root_locks()
        except BaseException:
            # The checked-out conn must never strand — even a constructor
            # failure (before the guard exists) returns it to the pool.
            if guard is not None:
                await guard.release()
            else:
                try:
                    await pool.putconn(conn)
                except Exception:
                    logger.warning(
                        "[WriterGuard] putconn after failed construction",
                        exc_info=True,
                    )
            raise
        guard._monitor_task = asyncio.create_task(
            guard._monitor(), name=f"writer-guard-monitor-{run_id[:8]}"
        )
        logger.info(
            f"[WriterGuard] fenced run={run_id} thread={thread_id} "
            f"(pool={pool.get_stats().get('pool_available', '?')} free)"
        )
        return guard

    async def _take_root_locks(self) -> None:
        t_key = thread_key(self.thread_id)
        n_key = namespace_key(self.thread_id, ROOT_NS)
        deadline = asyncio.get_running_loop().time() + LOCK_RETRY_WINDOW
        delay = 0.05
        while True:
            async with self.mutex:
                if not self._shared_held:
                    self._shared_held = await self._try_lock(
                        "pg_try_advisory_lock_shared", t_key
                    )
                if self._shared_held and not self._root_held:
                    self._root_held = await self._try_lock(
                        "pg_try_advisory_lock", n_key
                    )
            if self._shared_held and self._root_held:
                return
            if asyncio.get_running_loop().time() >= deadline:
                held_by = "thread guard" if not self._shared_held else "root writer"
                raise WriterGuardUnavailable(
                    f"advisory lock contended ({held_by}) for thread={self.thread_id}"
                )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 0.4)

    async def _try_lock(self, fn: str, key: int) -> bool:
        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"SELECT {fn}(%s) AS ok", (key,))
            row = await cur.fetchone()
            return bool(row and row["ok"])

    # ------------------------------------------------------------- session

    def ensure_alive(self) -> None:
        if self.lost or self._released or self.conn.closed:
            raise GuardSessionLost(
                f"writer session for run={self.run_id} is gone "
                f"(lost={self.lost}, released={self._released})"
            )

    def attach_abort(self, cb: Callable[[], Any]) -> None:
        """Register the graph-cancel callback the monitor fires on session loss.

        Fires immediately if the session was already lost before attachment
        (the monitor's failure path runs once and won't re-fire)."""
        self._abort_cb = cb
        if self.lost:
            try:
                cb()
            except Exception:
                logger.error("[WriterGuard] abort callback failed", exc_info=True)

    async def _monitor(self) -> None:
        """Detect session loss between saver boundaries and abort the run
        before it can act on a stale view. A ping failure is authoritative:
        session-level advisory locks cannot outlive their connection.

        ``lost`` is set while still HOLDING the mutex, so any mutex holder
        (the finalize CAS, saver ops) can trust a lost=False read for the
        whole of its critical section — the loss decision and the CAS are
        serialized, never racing."""
        while not self._released:
            await asyncio.sleep(MONITOR_INTERVAL)
            if self._released:
                return
            async with self.mutex:
                if self._released:
                    return
                try:
                    await asyncio.wait_for(
                        self.conn.execute("SELECT 1"), timeout=PING_TIMEOUT
                    )
                    continue
                except Exception:
                    self.lost = True
            logger.critical(
                f"[WriterGuard] session lost for run={self.run_id} "
                f"thread={self.thread_id}; aborting the run"
            )
            if self._abort_cb is not None:
                try:
                    self._abort_cb()
                except Exception:
                    logger.error(
                        "[WriterGuard] abort callback failed", exc_info=True
                    )
            return

    # ------------------------------------------------------------- teardown

    async def demote_to_tail(self) -> None:
        """Post-finalize with surviving subagent writers: drop N(root) so the
        thread's next turn can start, keep the session (shared T + saver)
        alive for the tail writers."""
        if self._released or not self._root_held:
            return
        if self.lost or self.conn.closed:
            return  # session death already dropped every lock server-side
        try:
            async with self.mutex:
                await self.conn.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (namespace_key(self.thread_id, ROOT_NS),),
                )
            self._root_held = False
        except Exception:
            self.lost = True
            logger.warning(
                f"[WriterGuard] demote failed for run={self.run_id}", exc_info=True
            )

    async def release(self, *, discard: bool = False) -> None:
        """Full teardown: unlock everything, stop the monitor, return the
        conn. Single-flight and cancellation-safe: the work runs in a
        shielded task, so a cancelled caller can neither abandon the cleanup
        half-way nor leave a later call believing it already ran.

        ``discard=True`` (or a lost session) closes the physical connection
        instead of recycling it: the run's saver still references the
        connection *object*, so the pool must never re-serve it — a zombie
        write would otherwise interleave with the next owner's session.
        The first call LATCHES the clean-vs-discard decision; a discard
        request arriving after a clean release started is logged and has no
        effect (a mid-flight flip could retarget the saver at the pool AND
        close the conn — the exact resurrection discard exists to prevent)."""
        if self._release_task is None:
            if discard:
                self._discard = True
            self._release_task = asyncio.create_task(
                self._do_release(), name=f"writer-guard-release-{self.run_id[:8]}"
            )
        elif discard and not self._discard:
            logger.warning(
                f"[WriterGuard] late discard request for run={self.run_id} "
                f"ignored; release already started clean"
            )
        await asyncio.shield(self._release_task)

    async def _do_release(self) -> None:
        self._released = True
        if self._monitor_task is not None and not self._monitor_task.done():
            self._monitor_task.cancel()
        self._root_held = False
        self._shared_held = False
        # Latched once: immutable for the rest of the release, so the
        # clean path's retarget and the discard path's close are exclusive.
        discard = self._discard or self.lost
        try:
            if not discard and not self.conn.closed:
                try:
                    async with self.mutex:
                        if self.lost:
                            # monitor confirmed loss while we queued
                            discard = True
                        else:
                            await self.conn.execute(
                                "SELECT pg_advisory_unlock_all()"
                            )
                            # Late readers (snapshot endpoints holding this
                            # run's graph) keep a reference to this saver:
                            # retarget it at the pool so nothing can reach
                            # the returned connection through it.
                            self.saver.conn = self._pool
                except Exception:
                    self.lost = True
                    discard = True
                    logger.warning(
                        f"[WriterGuard] unlock_all failed for run={self.run_id}",
                        exc_info=True,
                    )
        finally:
            if discard and not self.conn.closed:
                # Never recycle a conn a zombie saver can still reach: the
                # pool reset only unlocks/rolls back — the next checkout
                # would share the object with the dead run's saver. Close it
                # so late writes fail loudly and the pool replenishes.
                try:
                    await asyncio.wait_for(self.conn.close(), timeout=5.0)
                except Exception:
                    logger.warning(
                        f"[WriterGuard] close failed for run={self.run_id}; "
                        f"forcing socket shutdown",
                        exc_info=True,
                    )
                    try:
                        self.conn.pgconn.finish()
                    except Exception:
                        pass
            try:
                await self._pool.putconn(self.conn)
            except Exception:
                logger.warning(
                    f"[WriterGuard] putconn failed for run={self.run_id}",
                    exc_info=True,
                )
