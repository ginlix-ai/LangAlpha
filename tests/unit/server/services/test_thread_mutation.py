"""ThreadMutationRunner contracts (v4 2.4) — the exclusive-T fence for
compact/offload/delete.

Pins the settled behavior the handlers and admission depend on:

- gate order: local slot CAS → ledger (in_progress row refuses) → exclusive
  T(thread); every refusal is a typed MutationConflict with an honest code;
- pool exhaustion is MutationUnavailable (bounded 503), never an unfenced op;
- lock-contention classification: live run → workflow_active, foreign op key
  → compaction_in_progress, else thread_busy (tail writers);
- the fence lifecycle: fenced session carries conn+saver, unlock+putconn on
  every exit (clean, error), unlock failure closes the conn so the pool never
  re-serves a wedged session;
- auto-compaction windows are marker-only with strict ownership (open False
  when the slot is held; close ignores an exclusive op);
- request_stop: local exclusive cancelled in-process, remote exclusive
  signalled via the stop key, windows never stopped here;
- is_mutating / wait_until_idle see both local ops and foreign advertised
  op keys.

The PG lock itself (_take_exclusive_t) and Redis are mocked — the advisory
keyspace interplay with runs is Gate 2 (--workers 2) territory.
"""

import asyncio
import json
from contextlib import contextmanager, suppress
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import src.server.services.thread_mutation as tm
from src.server.services.thread_mutation import (
    MutationConflict,
    MutationUnavailable,
    ThreadMutationRunner,
    mutation_op_key,
    mutation_stop_key,
)

TL_DB = "src.server.database.runs.lifecycle"
SR_DB = "src.server.database.runs.subagent_runs"
WG = "src.server.services.writer_guard"


class _FakeRedis:
    """Minimal async Redis: get/set/expire plus the guarded-delete eval."""

    def __init__(self, store: dict | None = None):
        self.store = store if store is not None else {}
        self.set_calls: list = []

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ex=None):
        self.store[key] = value
        self.set_calls.append((key, value, ex))
        return True

    async def expire(self, key, ttl):
        return key in self.store

    async def eval(self, script, numkeys, key, arg):
        if self.store.get(key) == arg:
            del self.store[key]
            return 1
        return 0


def _make_runner(client=None) -> ThreadMutationRunner:
    """Fresh runner (never the singleton) with Redis stubbed out."""
    runner = ThreadMutationRunner()
    runner._client = MagicMock(return_value=client)
    return runner


def _foreign_op_payload(op_id: str = "foreign1", kind: str = "exclusive") -> str:
    return json.dumps({"op_id": op_id, "verb": "compact", "kind": kind, "pid": 1})


def _guard_off():
    return patch(f"{WG}.guard_enabled", return_value=False)


def _no_active_run():
    return patch(f"{TL_DB}.get_active_run", new=AsyncMock(return_value=None))


def _open_task_runs(count: int):
    """Live background subagent runs on the thread, as the delete gate sees
    them."""
    return patch(f"{SR_DB}.count_open_runs_for_thread", new=AsyncMock(return_value=count))


@contextmanager
def _fenced_env(pool, take_t: bool, saver=None):
    """Guard-enabled environment for an exclusive() run."""
    with (
        patch(f"{WG}.guard_enabled", return_value=True),
        patch(f"{WG}.get_writer_pool", return_value=pool),
        patch.object(
            ThreadMutationRunner,
            "_take_exclusive_t",
            new=AsyncMock(return_value=take_t),
        ),
        patch(
            "langgraph.checkpoint.postgres.aio.AsyncPostgresSaver",
            new=MagicMock(return_value=saver),
        ),
    ):
        yield


def _make_conn_pool():
    conn = MagicMock()
    conn.execute = AsyncMock()
    conn.close = AsyncMock()
    pool = MagicMock()
    pool.getconn = AsyncMock(return_value=conn)
    pool.putconn = AsyncMock()
    return conn, pool


class TestExclusiveGates:
    @pytest.mark.asyncio
    async def test_local_slot_refuses_second_op(self):
        """Two mutations in one process: the local CAS refuses the second
        before it touches the DB or the pool."""
        runner = _make_runner()
        with _guard_off(), _no_active_run():
            async with runner.exclusive("t1", "compact"):
                with pytest.raises(MutationConflict) as exc:
                    async with runner.exclusive("t1", "offload"):
                        pass  # pragma: no cover
        assert exc.value.detail["code"] == "compaction_in_progress"
        assert exc.value.detail["verb"] == "offload"
        assert "t1" not in runner._local

    @pytest.mark.asyncio
    async def test_ledger_gate_refuses_active_run_and_releases_slot(self):
        """An in_progress row — even a crashed one awaiting the recovery
        scanner — refuses the mutation; the local slot is released so a
        later attempt isn't poisoned."""
        runner = _make_runner()
        with (
            _guard_off(),
            patch(
                f"{TL_DB}.get_active_run",
                new=AsyncMock(return_value={"conversation_response_id": "r1"}),
            ),
        ):
            with pytest.raises(MutationConflict) as exc:
                async with runner.exclusive("t1", "compact"):
                    pass  # pragma: no cover
        assert exc.value.detail["code"] == "workflow_active"
        assert "t1" not in runner._local

        # Slot is free: the same thread accepts the next mutation.
        with _guard_off(), _no_active_run():
            async with runner.exclusive("t1", "compact"):
                assert "t1" in runner._local

    @pytest.mark.asyncio
    async def test_pool_exhaustion_maps_to_unavailable(self):
        pool = MagicMock()
        pool.getconn = AsyncMock(side_effect=TimeoutError("pool timeout"))
        runner = _make_runner()
        with (
            _no_active_run(),
            patch(f"{WG}.guard_enabled", return_value=True),
            patch(f"{WG}.get_writer_pool", return_value=pool),
        ):
            with pytest.raises(MutationUnavailable):
                async with runner.exclusive("t1", "compact"):
                    pass  # pragma: no cover
        assert "t1" not in runner._local

    @pytest.mark.asyncio
    async def test_lock_contention_classifies_workflow_active(self):
        """T(thread) held + a run appeared between the pre-gate and the lock
        attempt → workflow_active, and the conn goes back to the pool."""
        conn, pool = _make_conn_pool()
        runner = _make_runner()
        with (
            patch(
                f"{TL_DB}.get_active_run",
                new=AsyncMock(
                    side_effect=[None, {"conversation_response_id": "r1"}]
                ),
            ),
            _fenced_env(pool, take_t=False),
        ):
            with pytest.raises(MutationConflict) as exc:
                async with runner.exclusive("t1", "compact"):
                    pass  # pragma: no cover
        assert exc.value.detail["code"] == "workflow_active"
        pool.putconn.assert_awaited_once_with(conn)

    @pytest.mark.asyncio
    async def test_lock_contention_classifies_foreign_mutation(self):
        """T(thread) held + no run + a foreign advertised op key → the rival
        is another worker's mutation: compaction_in_progress."""
        conn, pool = _make_conn_pool()
        client = _FakeRedis({mutation_op_key("t1"): _foreign_op_payload()})
        runner = _make_runner(client)
        with _no_active_run(), _fenced_env(pool, take_t=False):
            with pytest.raises(MutationConflict) as exc:
                async with runner.exclusive("t1", "compact"):
                    pass  # pragma: no cover
        assert exc.value.detail["code"] == "compaction_in_progress"
        pool.putconn.assert_awaited_once_with(conn)

    @pytest.mark.asyncio
    async def test_lock_contention_classifies_thread_busy(self):
        """T(thread) held with no run and no rival op → tail subagent writers
        still hold shared T: thread_busy."""
        conn, pool = _make_conn_pool()
        runner = _make_runner()
        with _no_active_run(), _fenced_env(pool, take_t=False):
            with pytest.raises(MutationConflict) as exc:
                async with runner.exclusive("t1", "compact"):
                    pass  # pragma: no cover
        assert exc.value.detail["code"] == "thread_busy"
        pool.putconn.assert_awaited_once_with(conn)


class TestExclusiveFence:
    @pytest.mark.asyncio
    async def test_fenced_session_carries_conn_and_saver(self):
        """Fence and effect share one session: the yielded saver is bound to
        the locked conn; clean exit unlocks and returns the conn."""
        conn, pool = _make_conn_pool()
        fence_saver = object()
        client = _FakeRedis()
        runner = _make_runner(client)
        with _no_active_run(), _fenced_env(pool, take_t=True, saver=fence_saver):
            async with runner.exclusive("t1", "compact") as session:
                assert session.conn is conn
                assert session.saver is fence_saver
                # The op is advertised while held (give the task a tick).
                await asyncio.sleep(0)
                assert mutation_op_key("t1") in client.store
                assert await runner.is_mutating("t1")

        # Unlock ran on the fence conn, which went back to the pool.
        assert conn.execute.await_count == 1
        assert "pg_advisory_unlock" in conn.execute.await_args.args[0]
        pool.putconn.assert_awaited_once_with(conn)
        assert "t1" not in runner._local
        # The guarded delete clears the advertisement.
        await asyncio.sleep(0.01)
        assert mutation_op_key("t1") not in client.store

    @pytest.mark.asyncio
    async def test_fence_released_on_body_error(self):
        conn, pool = _make_conn_pool()
        runner = _make_runner()
        with _no_active_run(), _fenced_env(pool, take_t=True):
            with pytest.raises(RuntimeError):
                async with runner.exclusive("t1", "compact"):
                    raise RuntimeError("boom")
        assert conn.execute.await_count == 1  # unlock still ran
        pool.putconn.assert_awaited_once_with(conn)
        assert "t1" not in runner._local

    @pytest.mark.asyncio
    async def test_unlock_failure_closes_conn_before_putconn(self):
        """If the unlock can't be confirmed the conn must be closed — locks
        die with the connection — so the pool replenishes instead of
        re-serving a session that still holds exclusive T."""
        conn, pool = _make_conn_pool()
        conn.execute = AsyncMock(side_effect=ConnectionError("conn lost"))
        runner = _make_runner()
        with _no_active_run(), _fenced_env(pool, take_t=True):
            async with runner.exclusive("t1", "compact"):
                pass
        conn.close.assert_awaited_once()
        pool.putconn.assert_awaited_once_with(conn)

    @pytest.mark.asyncio
    async def test_guard_disabled_yields_unfenced_session(self):
        """Single-worker fallback: no conn, no saver — checkpoint writes go
        through the global pooled saver, exactly as unfenced runs do."""
        runner = _make_runner()
        with _guard_off(), _no_active_run(), _open_task_runs(0):
            async with runner.exclusive("t1", "delete") as session:
                assert session.conn is None
                assert session.saver is None
                assert "t1" in runner._local
        assert "t1" not in runner._local

    @pytest.mark.asyncio
    async def test_delete_refuses_while_a_background_task_run_is_live(self):
        """A background subagent outlives the turn that dispatched it, so the
        root-run gate can pass while task runs are still writing. Deleting the
        thread would cascade their ledger rows away under them."""
        runner = _make_runner()
        with _guard_off(), _no_active_run(), _open_task_runs(2):
            with pytest.raises(MutationConflict) as exc:
                async with runner.exclusive("t1", "delete"):
                    pass  # pragma: no cover
        assert exc.value.detail["code"] == "workflow_active"
        assert exc.value.detail["verb"] == "delete"
        assert "background task" in exc.value.detail["message"]
        # The refusal must not strand the local slot.
        assert "t1" not in runner._local

    @pytest.mark.asyncio
    async def test_compact_ignores_live_task_runs(self):
        """Only verbs that delete rows gate on the cascade. Compact/offload
        rewrite checkpoint state and leave the ledger intact, so a live
        background task is none of their business."""
        runner = _make_runner()
        probe = AsyncMock(return_value=5)
        with (
            _guard_off(),
            _no_active_run(),
            patch(f"{SR_DB}.count_open_runs_for_thread", new=probe),
        ):
            async with runner.exclusive("t1", "compact"):
                pass
        probe.assert_not_awaited()


class TestWindow:
    @pytest.mark.asyncio
    async def test_open_close_ownership(self):
        runner = _make_runner()
        assert runner.open_auto_compaction_window("t1") is True
        # Second open while held: not the owner, must not close later.
        assert runner.open_auto_compaction_window("t1") is False
        assert await runner.is_mutating("t1")
        runner.close_auto_compaction_window("t1")
        assert not await runner.is_mutating("t1")
        # Freed: the next turn's window opens from scratch.
        assert runner.open_auto_compaction_window("t1") is True
        runner.close_auto_compaction_window("t1")

    @pytest.mark.asyncio
    async def test_close_window_ignores_exclusive_op(self):
        """A stray window-close (streaming handler's finally) must never
        release a manual mutation's slot."""
        runner = _make_runner()
        with _guard_off(), _no_active_run():
            async with runner.exclusive("t1", "compact"):
                runner.close_auto_compaction_window("t1")
                assert "t1" in runner._local
        assert "t1" not in runner._local


class TestRequestStop:
    @pytest.mark.asyncio
    async def test_stop_cancels_local_exclusive(self):
        runner = _make_runner()

        async def _hold():
            with _guard_off(), _no_active_run():
                async with runner.exclusive("t1", "compact"):
                    await asyncio.sleep(60)

        task = asyncio.create_task(_hold())
        for _ in range(100):
            if "t1" in runner._local:
                break
            await asyncio.sleep(0.01)
        assert "t1" in runner._local

        assert await runner.request_stop("t1") == "cancelled"
        with pytest.raises(asyncio.CancelledError):
            await task
        assert "t1" not in runner._local

    @pytest.mark.asyncio
    async def test_stop_never_touches_a_window(self):
        """AUTO compaction is stopped through its turn's cancel path; the
        runner reports none and leaves the window open."""
        runner = _make_runner()
        runner.open_auto_compaction_window("t1")
        assert await runner.request_stop("t1") == "none"
        assert await runner.is_mutating("t1")
        runner.close_auto_compaction_window("t1")

    @pytest.mark.asyncio
    async def test_stop_signals_remote_exclusive(self):
        """Op owned by another worker: flag the stop key its heartbeat
        polls."""
        client = _FakeRedis({mutation_op_key("t1"): _foreign_op_payload("op9")})
        runner = _make_runner(client)
        assert await runner.request_stop("t1") == "signalled"
        assert client.store.get(mutation_stop_key("op9")) == "1"

    @pytest.mark.asyncio
    async def test_stop_none_when_idle(self):
        runner = _make_runner()
        assert await runner.request_stop("t1") == "none"


class TestLiveness:
    @pytest.mark.asyncio
    async def test_is_mutating_sees_local_and_remote(self):
        remote = _make_runner(
            _FakeRedis({mutation_op_key("t1"): _foreign_op_payload()})
        )
        assert await remote.is_mutating("t1") is True

        idle = _make_runner()
        assert await idle.is_mutating("t1") is False

    @pytest.mark.asyncio
    async def test_wait_until_idle_returns_when_local_op_finishes(self):
        runner = _make_runner()
        runner.open_auto_compaction_window("t1")

        async def _close_soon():
            await asyncio.sleep(0.05)
            runner.close_auto_compaction_window("t1")

        closer = asyncio.create_task(_close_soon())
        assert await runner.wait_until_idle("t1", timeout=1.0) is True
        await closer

    @pytest.mark.asyncio
    async def test_wait_until_idle_times_out_on_held_thread(self):
        runner = _make_runner()
        runner.open_auto_compaction_window("t1")
        assert await runner.wait_until_idle("t1", timeout=0.05) is False
        runner.close_auto_compaction_window("t1")

    @pytest.mark.asyncio
    async def test_wait_until_idle_polls_foreign_key(self, monkeypatch):
        monkeypatch.setattr(tm, "IDLE_POLL_INTERVAL_S", 0.01)
        store = {mutation_op_key("t1"): _foreign_op_payload()}
        runner = _make_runner(_FakeRedis(store))
        assert await runner.wait_until_idle("t1", timeout=0.05) is False
        store.clear()
        assert await runner.wait_until_idle("t1", timeout=0.5) is True


class TestHeartbeat:
    @pytest.mark.asyncio
    async def test_heartbeat_polls_stop_flag_and_cancels_op_task(
        self, monkeypatch
    ):
        """The heartbeat doubles as the cross-worker stop listener: a stop
        key flagged by another worker's /cancel lands as a local task
        cancel within one heartbeat interval."""
        monkeypatch.setattr(tm, "HEARTBEAT_INTERVAL_S", 0.01)
        started = asyncio.Event()

        async def _hang():
            started.set()
            await asyncio.sleep(60)

        victim = asyncio.create_task(_hang())
        await started.wait()

        op = tm._LocalOp(
            op_id="op-stop",
            verb="compact",
            kind="exclusive",
            done=asyncio.Event(),
            task=victim,
        )
        op.payload = ThreadMutationRunner._build_payload(op)
        client = _FakeRedis({mutation_stop_key("op-stop"): "1"})
        runner = _make_runner(client)

        heartbeat = asyncio.create_task(runner._advertise("t1", op))
        with suppress(asyncio.CancelledError):
            await asyncio.wait_for(victim, timeout=1.0)
        assert victim.cancelled()

        op.done.set()
        heartbeat.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat
