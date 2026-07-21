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
import contextlib
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
# Each in-flight write is one SQL op on a live conn; a drain that outlasts
# this is a wedged session and the transition fails toward discard/keep-lock.
WRITE_DRAIN_TIMEOUT = 30.0

# LangGraph's checkpoint-namespace separator (langgraph.constants.NS_SEP);
# duplicated as a literal so importing this module doesn't pull the langgraph
# package init.
_NS_SEP = "|"


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


async def held_task_namespaces(
    thread_id: str, task_ids: list[str]
) -> set[str] | None:
    """Which of these tasks' N(thread, task:{id}) locks are held right now,
    across every worker of this database — a read of pg_locks, never an
    acquisition, so it cannot refuse a starting subagent's fence. Scoped to
    current_database(): advisory-lock identity is database-local and
    pg_locks is cluster-global. Held ⇒ a live writer owns the namespace;
    free ⇒ settled, or its worker died (session advisory locks release on
    disconnect). None ⇒ probe failed (callers keep, not filter).
    """
    if not task_ids:
        return set()
    # pg_locks splits a 64-bit advisory key into (classid=high32, objid=low32)
    # with objsubid=1; match on the unsigned halves of our signed keys.
    pair_to_task: dict[tuple[int, int], str] = {}
    for task_id in task_ids:
        unsigned = namespace_key(thread_id, f"task:{task_id}") & 0xFFFFFFFFFFFFFFFF
        pair_to_task[(unsigned >> 32, unsigned & 0xFFFFFFFF)] = task_id
    try:
        from src.server.database import conversation as qr_db

        async with qr_db.get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT classid::bigint, objid::bigint FROM pg_locks
                    WHERE locktype = 'advisory' AND granted AND objsubid = 1
                      AND database = (
                        SELECT oid FROM pg_database
                        WHERE datname = current_database()
                      )
                    """
                )
                rows = await cur.fetchall()
    except Exception:
        logger.warning(
            f"[WriterGuard] pg_locks probe failed for thread={thread_id}",
            exc_info=True,
        )
        return None
    return {
        pair_to_task[(classid, objid)]
        for classid, objid in rows
        if (classid, objid) in pair_to_task
    }


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
# Namespace-fenced saver
# --------------------------------------------------------------------------

_fenced_saver_cls = None


def _get_fenced_saver_cls():
    """Build (once) the saver subclass that enforces the namespace seal.

    Lazy like the saver import itself: langgraph is only loaded when a guard
    is actually constructed.
    """
    global _fenced_saver_cls
    if _fenced_saver_cls is not None:
        return _fenced_saver_cls

    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    class _FencedSaver(AsyncPostgresSaver):
        """The run's saver, sealed at finalize (I2): once the guard drops
        N(root), only surviving task namespaces this session still owns may
        write — a late root-namespace write would dual-write with whichever
        session holds N(root) now.

        ``fenced_write`` (not a bare pre-check) because the base saver
        serializes params in a thread BEFORE taking its I/O lock: a write
        authorized here can be suspended across a seal + lock drop and land
        after the fence fell. The guard tracks it as in-flight, and every
        fence transition drains in-flight writes before dropping locks."""

        guard: "WriterGuard" = None  # set right after construction

        async def aput(self, config, checkpoint, metadata, new_versions):
            async with self.guard.fenced_write(config):
                return await super().aput(config, checkpoint, metadata, new_versions)

        async def aput_writes(self, config, writes, task_id, task_path=""):
            async with self.guard.fenced_write(config):
                return await super().aput_writes(config, writes, task_id, task_path)

    _fenced_saver_cls = _FencedSaver
    return _FencedSaver


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
        self._pool = pool
        self.conn = conn
        self.thread_id = thread_id
        self.run_id = run_id
        # THE one I/O mutex: installed as the saver's own lock so saver ops
        # and lifecycle SQL on this connection can never interleave.
        self.mutex = asyncio.Lock()
        self.saver = _get_fenced_saver_cls()(conn)
        self.saver.guard = self
        self.saver.lock = self.mutex
        self.lost = False
        self.root_sealed = False
        self._task_ns: set[str] = set()
        # In-flight authorized saver writes, counted per top-level namespace
        # segment ('' = root). Fence transitions drain the segments they are
        # about to unfence, so an authorized write can never land after its
        # lock dropped.
        self._inflight_ns: dict[str, int] = {}
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

    # ------------------------------------------------------- task namespaces

    async def acquire_task_ns(self, task_id: str) -> bool:
        """Take exclusive N(thread, task:{id}) on this run's session — one
        live writer per background-subagent namespace, cluster-wide.

        Idempotent per session (a task_id already held returns True without
        stacking a second server-side lock). Fails closed: an unusable
        session or a query error refuses the namespace rather than admitting
        an unfenced writer.
        """
        if self.lost or self._released or self.conn.closed:
            return False
        key = namespace_key(self.thread_id, f"task:{task_id}")
        try:
            # Membership check + lock + record under ONE mutex hold: two
            # concurrent acquires of the same task_id must not both reach
            # the server (session advisory locks stack, and a single
            # release would then leave a phantom second hold).
            async with self.mutex:
                if task_id in self._task_ns:
                    return True
                ok = await self._try_lock("pg_try_advisory_lock", key)
                if ok:
                    self._task_ns.add(task_id)
        except Exception:
            logger.warning(
                f"[WriterGuard] task-ns acquire failed for run={self.run_id} "
                f"task={task_id}; refusing (fail closed)",
                exc_info=True,
            )
            return False
        return ok

    def owns_task_ns(self, task_id: str) -> bool:
        """True while this session may still write ns=task:{task_id} — the
        authoritative "is this writer mine" test for teardown decisions."""
        return task_id in self._task_ns

    async def release_task_ns(self, task_id: str) -> None:
        """Best-effort drop of N(thread, task:{id}) once the task's writer
        settled. Removed from the held set first, so post-seal writes to the
        namespace refuse even if the server-side unlock fails (the session's
        eventual unlock_all/close reclaims it)."""
        if task_id not in self._task_ns:
            return
        self._task_ns.discard(task_id)
        if self.lost or self._released or self.conn.closed:
            return
        # A write authorized before the permit fell may still be in flight
        # (e.g. a double-cancelled writer's abandoned saver task): keep the
        # server-side lock until it lands, or keep it forever — never hand
        # the namespace to another session over a pending local write. A
        # writer that had NOT yet authorized (queued behind the mutex) is
        # refused by check_write_ns once the membership above is gone, so
        # the counted set here is exhaustive.
        top = f"task:{task_id}"
        if not await self._drain_ns_writes(lambda ns: ns == top):
            logger.critical(
                f"[WriterGuard] task-ns write still in flight after "
                f"{WRITE_DRAIN_TIMEOUT:.0f}s for run={self.run_id} "
                f"task={task_id}; keeping the lock (fail closed)"
            )
            return
        try:
            async with self.mutex:
                # Re-checked after the drain await: membership fell above, so
                # the tail drain no longer waits on this writer and the full
                # release can complete first — its unlock_all reclaims the
                # lock, and the conn may already be back in the pool (or
                # serving another session). Executing here then would fire a
                # stray unlock on a connection this guard no longer owns.
                if self.lost or self._released or self.conn.closed:
                    return
                await self.conn.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (namespace_key(self.thread_id, f"task:{task_id}"),),
                )
        except Exception:
            logger.warning(
                f"[WriterGuard] task-ns unlock failed for run={self.run_id} "
                f"task={task_id}",
                exc_info=True,
            )

    @staticmethod
    def _top_ns(config) -> str:
        ns = str(
            ((config or {}).get("configurable") or {}).get("checkpoint_ns") or ""
        )
        return ns.split(_NS_SEP)[0]

    def check_write_ns(self, config) -> None:
        """Saver write gate: a task-namespace write requires that namespace's
        lock to still be held (membership survives until release), the root
        namespace requires an unsealed session. Task ownership is checked
        even while root is unsealed — a saver that queued behind the mutex
        before its task settled would otherwise authorize uncounted after
        ``release_task_ns`` handed the lock to a successor."""
        top = self._top_ns(config)
        if top.startswith("task:"):
            if top[5:] in self._task_ns:
                return
            raise GuardSessionLost(
                f"run={self.run_id}: checkpoint write to ns={top!r} refused "
                f"(this session no longer owns that task namespace)"
            )
        if self.root_sealed:
            raise GuardSessionLost(
                f"run={self.run_id} is sealed: checkpoint write to ns={top!r} "
                f"refused (this session no longer owns the root namespace)"
            )

    @contextlib.asynccontextmanager
    async def fenced_write(self, config):
        """Authorize a saver write atomically with the fence state and track
        it until its SQL lands. Authorization happens under the I/O mutex —
        the same exclusion the seal transitions use — and fence transitions
        drain in-flight writes before dropping any lock, so 'authorized'
        always implies 'lands inside the fence'."""
        top = self._top_ns(config)
        async with self.mutex:
            self.check_write_ns(config)
            self._inflight_ns[top] = self._inflight_ns.get(top, 0) + 1
        try:
            yield
        finally:
            n = self._inflight_ns.get(top, 1) - 1
            if n > 0:
                self._inflight_ns[top] = n
            else:
                self._inflight_ns.pop(top, None)

    async def _drain_ns_writes(self, pred: Callable[[str], bool]) -> bool:
        """Wait until no in-flight write targets a namespace matching
        ``pred``. Returns False on deadline — the caller must then fail
        toward keeping the lock or discarding the session, never toward
        unfencing the pending write."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + WRITE_DRAIN_TIMEOUT
        while any(pred(ns) for ns in self._inflight_ns):
            if loop.time() >= deadline:
                return False
            await asyncio.sleep(0.01)
        return True

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
        # Seal BEFORE dropping N(root): no window where another session can
        # own the root namespace while this saver still accepts root writes.
        self.root_sealed = True
        if self.lost or self.conn.closed:
            return  # session death already dropped every lock server-side
        # Root writes authorized before the seal must land before the lock
        # drops (task-ns writes may keep flowing — their locks stay held).
        if not await self._drain_ns_writes(lambda ns: not ns.startswith("task:")):
            self.lost = True
            logger.critical(
                f"[WriterGuard] root write still in flight after "
                f"{WRITE_DRAIN_TIMEOUT:.0f}s at demote for run={self.run_id}; "
                f"failing the session (locks die with the discarded conn)"
            )
            return
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
        # After release the saver survives only for late READERS (snapshot
        # endpoints); seal with no owned namespaces so any straggler write
        # fails loudly instead of landing unfenced via the retargeted pool.
        self.root_sealed = True
        self._task_ns.clear()
        # Latched once: immutable for the rest of the release, so the
        # clean path's retarget and the discard path's close are exclusive.
        discard = self._discard or self.lost
        # A write authorized pre-seal may still be in flight; the clean path
        # must not unlock/retarget over it. Timeout flips to discard so the
        # closed conn fails the straggler loudly.
        if not discard and not await self._drain_ns_writes(lambda _ns: True):
            discard = True
            logger.critical(
                f"[WriterGuard] write still in flight after "
                f"{WRITE_DRAIN_TIMEOUT:.0f}s at release for run={self.run_id}; "
                f"discarding the session"
            )
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
