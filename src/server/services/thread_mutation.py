"""Thread mutations v4 Phase 2.4 — the exclusive-T fence for compact/offload/delete.

A mutation takes exclusive T(thread) in the same advisory keyspace runs use
(every fenced run and tail-writer session holds T shared), so a mutation and
a live checkpoint writer structurally cannot overlap — across workers, with
no cooperative check. The Redis op-liveness key is coordination only:
admission sees "compacting" instead of a bare lock 503, /cancel can stop a
mutation owned by another worker, and a running turn's AUTO compaction
advertises its window through the same key. Correctness never depends on
Redis: the lock and the ledger gate carry it alone.
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, Literal, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)

__all__ = [
    "ThreadMutationRunner",
    "MutationSession",
    "MutationConflict",
    "MutationUnavailable",
    "mutation_op_key",
    "mutation_stop_key",
]

OP_KEY_TTL_S = 30
HEARTBEAT_INTERVAL_S = 5.0
STOP_FLAG_TTL_S = 120
IDLE_POLL_INTERVAL_S = 1.0

# Delete the op key only if it still holds OUR payload: a slow close racing a
# successor's open must not erase the successor's advertisement.
_DEL_IF_VALUE_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then "
    "return redis.call('del', KEYS[1]) end return 0"
)


def mutation_op_key(thread_id: str) -> str:
    return f"thread:mutation:{thread_id}"


def mutation_stop_key(op_id: str) -> str:
    return f"thread:mutation:stop:{op_id}"


class MutationConflict(Exception):
    """The thread is busy — maps to HTTP 409; ``detail`` is the response body."""

    def __init__(self, code: str, verb: str, message: str):
        self.detail = {"code": code, "verb": verb, "message": message}
        super().__init__(message)


class MutationUnavailable(Exception):
    """Pinned-session budget exhausted — bounded 503, like WriterGuardUnavailable."""


# Verbs whose mutation deletes rows that subagent_runs cascades from. These
# alone gate on live task runs; compact/offload rewrite ONLY root-namespace
# checkpoint state (never task:*) and leave the ledger intact — and a live
# tail writer still holds shared T(thread), so the exclusive-T take below
# refuses them with thread_busy whenever the guard is enabled.
CASCADING_VERBS = frozenset({"delete"})


@dataclass
class MutationSession:
    """One held mutation: the locked conn (and a saver on it) when fenced.

    ``conn``/``saver`` are None in single-worker fallback mode (guard
    disabled) — checkpoint writes then go through the global pooled saver,
    exactly as runs do there.
    """

    op_id: str
    conn: Optional[Any] = None
    saver: Optional[Any] = None


@dataclass
class _LocalOp:
    op_id: str
    verb: str
    kind: str  # "exclusive" | "window"
    done: asyncio.Event
    payload: str = ""
    task: Optional[asyncio.Task] = None  # stop target (exclusive only)
    heartbeat: Optional[asyncio.Task] = None


class ThreadMutationRunner:
    """Process singleton; all cross-worker state lives in PG locks + Redis."""

    _instance: Optional["ThreadMutationRunner"] = None

    def __init__(self) -> None:
        self._local: Dict[str, _LocalOp] = {}

    @classmethod
    def get_instance(cls) -> "ThreadMutationRunner":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ------------------------------------------------------------ redis (UX)

    @staticmethod
    def _client():
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if cache.enabled and cache.client:
            return cache.client
        return None

    async def _read_op(self, thread_id: str) -> Optional[dict]:
        client = self._client()
        if client is None:
            return None
        try:
            raw = await client.get(mutation_op_key(thread_id))
            return json.loads(raw) if raw else None
        except Exception:
            return None

    async def _advertise(self, thread_id: str, op: _LocalOp) -> None:
        """SET the op key, then keep it fresh; exclusive ops also poll their
        stop flag so a /cancel landing on another worker interrupts this one.
        Best-effort throughout — the PG lock is the fence, not this key."""
        key = mutation_op_key(thread_id)
        client = self._client()
        if client is not None:
            try:
                await client.set(key, op.payload, ex=OP_KEY_TTL_S)
            except Exception:
                pass
        while not op.done.is_set():
            await asyncio.sleep(HEARTBEAT_INTERVAL_S)
            if op.done.is_set():
                return
            client = self._client()
            if client is None:
                continue
            try:
                refreshed = await client.expire(key, OP_KEY_TTL_S)
                if not refreshed:
                    # Expired under us (Redis blip); we still own the thread —
                    # no rival can exist while our lock/local slot is held.
                    await client.set(key, op.payload, ex=OP_KEY_TTL_S)
                if op.kind == "exclusive":
                    stop = await client.get(mutation_stop_key(op.op_id))
                    if stop and op.task is not None and not op.task.done():
                        logger.info(
                            f"[ThreadMutation] cross-worker stop for "
                            f"{op.verb} on thread={thread_id}"
                        )
                        op.task.cancel()
            except Exception:
                continue

    def _finish(self, thread_id: str, op: _LocalOp) -> None:
        """Idempotent local close: release waiters, stop the heartbeat, and
        schedule the guarded key delete."""
        if self._local.get(thread_id) is op:
            del self._local[thread_id]
        if op.done.is_set():
            return
        op.done.set()
        if op.heartbeat is not None and not op.heartbeat.done():
            op.heartbeat.cancel()
        client = self._client()
        if client is not None:

            async def _delete() -> None:
                try:
                    await client.eval(
                        _DEL_IF_VALUE_LUA, 1, mutation_op_key(thread_id), op.payload
                    )
                except Exception:
                    pass

            asyncio.create_task(
                _delete(), name=f"mutation-key-del-{op.op_id[:8]}"
            )

    @staticmethod
    def _build_payload(op: _LocalOp) -> str:
        return json.dumps(
            {"op_id": op.op_id, "verb": op.verb, "kind": op.kind, "pid": os.getpid()}
        )

    # ------------------------------------------------------------- queries

    async def is_mutating(self, thread_id: str) -> bool:
        if thread_id in self._local:
            return True
        return await self._read_op(thread_id) is not None

    async def wait_until_idle(self, thread_id: str, timeout: float) -> bool:
        """True once no mutation (local or advertised) holds the thread;
        False if one still does at the deadline."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            op = self._local.get(thread_id)
            if op is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return False
                try:
                    await asyncio.wait_for(op.done.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    return False
                continue  # re-check: a follow-up op may have opened
            if await self._read_op(thread_id) is None:
                return True
            if loop.time() >= deadline:
                return False
            await asyncio.sleep(IDLE_POLL_INTERVAL_S)

    # --------------------------------------------------------------- stop

    async def request_stop(
        self, thread_id: str
    ) -> Literal["cancelled", "signalled", "none"]:
        """Stop an EXCLUSIVE mutation: cancel it in-process, or flag the stop
        key its owning worker's heartbeat polls. Auto-compaction windows are
        stopped through their turn's own cancel path, never here."""
        op = self._local.get(thread_id)
        if op is not None and op.kind == "exclusive":
            if op.task is not None and not op.task.done():
                op.task.cancel()
                return "cancelled"
            return "none"
        remote = await self._read_op(thread_id)
        if remote and remote.get("kind") == "exclusive" and remote.get("op_id"):
            client = self._client()
            if client is not None:
                try:
                    await client.set(
                        mutation_stop_key(remote["op_id"]), "1", ex=STOP_FLAG_TTL_S
                    )
                    return "signalled"
                except Exception:
                    pass
        return "none"

    # ------------------------------------------------------------- window

    def open_window(self, thread_id: str) -> bool:
        """Advertise a running turn's AUTO compaction (admission holds new
        sends until it closes). Marker only — the turn's own WriterGuard is
        the fence. False = another op already holds the thread (a manual
        mutation); the caller must not close a window it didn't open."""
        if thread_id in self._local:
            return False
        op = _LocalOp(
            op_id=uuid4().hex,
            verb="auto_compact",
            kind="window",
            done=asyncio.Event(),
        )
        op.payload = self._build_payload(op)
        self._local[thread_id] = op
        op.heartbeat = asyncio.create_task(
            self._advertise(thread_id, op), name=f"mutation-window-{op.op_id[:8]}"
        )
        return True

    def close_window(self, thread_id: str) -> None:
        op = self._local.get(thread_id)
        if op is None or op.kind != "window":
            return
        self._finish(thread_id, op)

    # ----------------------------------------------------------- exclusive

    @asynccontextmanager
    async def exclusive(
        self, thread_id: str, verb: str
    ) -> AsyncIterator[MutationSession]:
        """Hold the thread for a standalone mutation (compact/offload/delete).

        Gate order: local slot → ledger (an in_progress row refuses, even a
        crashed one — the recovery scanner settles those first) → exclusive
        T(thread) on a writer-pool conn. Checkpoint writes belong on the
        yielded ``saver`` so the fence and the writes share one session.
        """
        from src.server.database import subagent_runs as sr_db
        from src.server.database import turn_lifecycle as tl_db
        from src.server.services import writer_guard as wg

        if thread_id in self._local:
            raise MutationConflict(
                "compaction_in_progress",
                verb,
                "Another operation is already running on this thread. "
                "Wait for it to finish, then try again.",
            )
        op = _LocalOp(
            op_id=uuid4().hex,
            verb=verb,
            kind="exclusive",
            done=asyncio.Event(),
            task=asyncio.current_task(),
        )
        op.payload = self._build_payload(op)
        self._local[thread_id] = op

        conn = None
        pool = None
        locked = False
        try:
            active = await tl_db.get_active_run(thread_id)
            if active:
                raise MutationConflict(
                    "workflow_active",
                    verb,
                    f"Cannot {verb} while a response is streaming on this "
                    "thread. Wait for the current turn to finish, then try "
                    "again.",
                )

            if verb in CASCADING_VERBS:
                # A background subagent outlives its dispatching turn, so the
                # root-run gate above can pass while task runs are still live.
                # Deleting the thread cascades their ledger rows away under
                # them; refuse with the same 409 the frontend already handles.
                open_tasks = await sr_db.count_open_runs_for_thread(thread_id)
                if open_tasks:
                    raise MutationConflict(
                        "workflow_active",
                        verb,
                        f"Cannot {verb} while {open_tasks} background task(s) "
                        "are still running on this thread. Wait for them to "
                        "finish, then try again.",
                    )

            saver = None
            if wg.guard_enabled():
                pool = wg.get_writer_pool()
                try:
                    conn = await pool.getconn(timeout=wg.CHECKOUT_TIMEOUT)
                except Exception:
                    raise MutationUnavailable(
                        f"pinned-session budget exhausted; cannot {verb} now"
                    )
                locked = await self._take_exclusive_t(conn, thread_id)
                if not locked:
                    # Someone holds T: a fenced run (shared), tail writers
                    # (shared), or a foreign mutation (exclusive). Classify
                    # for an honest 409.
                    active = await tl_db.get_active_run(thread_id)
                    if active:
                        raise MutationConflict(
                            "workflow_active",
                            verb,
                            f"Cannot {verb} while a response is streaming on "
                            "this thread. Wait for the current turn to "
                            "finish, then try again.",
                        )
                    if await self._read_op(thread_id) is not None:
                        raise MutationConflict(
                            "compaction_in_progress",
                            verb,
                            "Another operation is already running on this "
                            "thread. Wait for it to finish, then try again.",
                        )
                    raise MutationConflict(
                        "thread_busy",
                        verb,
                        "Background tasks are still writing on this thread. "
                        "Wait for them to finish, then try again.",
                    )
                from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

                saver = AsyncPostgresSaver(conn)

            op.heartbeat = asyncio.create_task(
                self._advertise(thread_id, op),
                name=f"mutation-{verb}-{op.op_id[:8]}",
            )
            logger.info(
                f"[ThreadMutation] {verb} holds thread={thread_id} "
                f"(fenced={conn is not None})"
            )
            yield MutationSession(op_id=op.op_id, conn=conn, saver=saver)
        finally:
            self._finish(thread_id, op)
            if conn is not None:
                try:
                    if locked:
                        await conn.execute(
                            "SELECT pg_advisory_unlock(%s)",
                            (wg.thread_key(thread_id),),
                        )
                except Exception:
                    # Locks die with the connection; close it so the pool
                    # replenishes instead of re-serving a wedged session.
                    logger.warning(
                        f"[ThreadMutation] unlock failed for thread={thread_id}",
                        exc_info=True,
                    )
                    try:
                        await asyncio.wait_for(conn.close(), timeout=5.0)
                    except Exception:
                        pass
                finally:
                    try:
                        await pool.putconn(conn)
                    except Exception:
                        logger.warning(
                            f"[ThreadMutation] putconn failed for "
                            f"thread={thread_id}",
                            exc_info=True,
                        )

    async def _take_exclusive_t(self, conn, thread_id: str) -> bool:
        """Try-acquire exclusive T with a brief retry so a just-finalized
        run's lock release can land; never blocks unboundedly."""
        from psycopg.rows import dict_row

        from src.server.services import writer_guard as wg

        key = wg.thread_key(thread_id)
        deadline = asyncio.get_running_loop().time() + wg.LOCK_RETRY_WINDOW
        delay = 0.05
        while True:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT pg_try_advisory_lock(%s) AS ok", (key,))
                row = await cur.fetchone()
                if row and row["ok"]:
                    return True
            if asyncio.get_running_loop().time() >= deadline:
                return False
            await asyncio.sleep(delay)
            delay = min(delay * 2, 0.4)
