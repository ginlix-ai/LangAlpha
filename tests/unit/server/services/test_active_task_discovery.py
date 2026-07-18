"""Locks the cross-worker active-subagent discovery contract.

``/status`` must list tail-mode subagents from ANY worker: membership in
``subagent:active:{thread}`` (maintained by ``write_task_meta``) is
enumerated, then verified against each task's N(thread, task:id) advisory
lock read from pg_locks — held means a live writer, free means settled or
its worker died. Probe failure keeps members; Redis-unknown falls back to
the local registry.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
    read_active_task_ids,
)
from src.server.services.background_task_manager import (
    BackgroundTaskManager,
)
from src.server.services.writer_guard import held_task_namespaces, namespace_key


# ---------------------------------------------------------------------------
# pg_locks key math — wire-verified against a live Postgres (classid=high32,
# objid=low32 of the unsigned key, objsubid=1); this pins the split.
# ---------------------------------------------------------------------------


def test_pg_locks_pair_reconstructs_namespace_key():
    key = namespace_key("t-probe-check", "task:AbCd12")
    unsigned = key & 0xFFFFFFFFFFFFFFFF
    classid, objid = unsigned >> 32, unsigned & 0xFFFFFFFF
    reconstructed = (classid << 32) | objid
    if reconstructed >= 2**63:
        reconstructed -= 2**64
    assert reconstructed == key


# ---------------------------------------------------------------------------
# held_task_namespaces
# ---------------------------------------------------------------------------


def _mock_db_conn(rows):
    cur = MagicMock()
    cur.execute = AsyncMock()
    cur.fetchall = AsyncMock(return_value=rows)
    cur_cm = MagicMock()
    cur_cm.__aenter__ = AsyncMock(return_value=cur)
    cur_cm.__aexit__ = AsyncMock(return_value=False)
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cur_cm)
    conn_cm = MagicMock()
    conn_cm.__aenter__ = AsyncMock(return_value=conn)
    conn_cm.__aexit__ = AsyncMock(return_value=False)
    return conn_cm


def _lock_row(thread_id: str, task_id: str) -> tuple[int, int]:
    unsigned = namespace_key(thread_id, f"task:{task_id}") & 0xFFFFFFFFFFFFFFFF
    return (unsigned >> 32, unsigned & 0xFFFFFFFF)


class TestHeldTaskNamespaces:
    @pytest.mark.asyncio
    async def test_filters_to_tasks_whose_lock_is_granted(self):
        rows = [_lock_row("t1", "aaa111"), (12345, 67890)]
        with patch(
            "src.server.database.conversation.get_db_connection",
            return_value=_mock_db_conn(rows),
        ):
            held = await held_task_namespaces("t1", ["aaa111", "bbb222"])
        assert held == {"aaa111"}

    @pytest.mark.asyncio
    async def test_empty_input_never_touches_the_db(self):
        with patch(
            "src.server.database.conversation.get_db_connection",
            side_effect=AssertionError("must not connect"),
        ):
            assert await held_task_namespaces("t1", []) == set()

    @pytest.mark.asyncio
    async def test_probe_failure_returns_none(self):
        with patch(
            "src.server.database.conversation.get_db_connection",
            side_effect=RuntimeError("db down"),
        ):
            assert await held_task_namespaces("t1", ["aaa111"]) is None


# ---------------------------------------------------------------------------
# write_task_meta maintains the active set in the same pipeline
# ---------------------------------------------------------------------------


class _FakePipe:
    def __init__(self):
        self.calls: list[tuple] = []

    def hset(self, *a, **kw):
        self.calls.append(("hset", a[0]))

    def expire(self, key, ttl):
        self.calls.append(("expire", key))

    def sadd(self, key, member):
        self.calls.append(("sadd", key, member))

    def srem(self, key, member):
        self.calls.append(("srem", key, member))

    async def execute(self):
        return []


def _fake_cache(pipe):
    cache = MagicMock()
    cache.enabled = True
    cache.client = MagicMock()
    cache.client.pipeline = MagicMock(return_value=pipe)
    return cache


def _task(task_id: str = "aaa111") -> SimpleNamespace:
    return SimpleNamespace(
        tool_call_id="tc-1",
        task_id=task_id,
        subagent_type="research",
        description="look things up",
        spawned_run_id="run-1",
        task_run_id=None,
    )


class TestActiveSetMaintenance:
    async def _write(self, status: str, *, fenced: bool = True) -> _FakePipe:
        registry = BackgroundTaskRegistry(thread_id="t1")
        pipe = _FakePipe()
        with (
            patch(
                "src.utils.cache.redis_cache.get_cache_client",
                return_value=_fake_cache(pipe),
            ),
            patch(
                "src.config.settings.get_redis_ttl_workflow_events",
                return_value=3600,
            ),
        ):
            await registry.write_task_meta(_task(), status, fenced=fenced)
        return pipe

    @pytest.mark.asyncio
    async def test_running_adds_member(self):
        pipe = await self._write("running")
        assert ("sadd", "subagent:active:t1", "aaa111") in pipe.calls
        assert ("expire", "subagent:active:t1") in pipe.calls

    @pytest.mark.asyncio
    async def test_unfenced_running_never_advertises(self):
        # No lock to verify against — a lockless member would always
        # classify as dead. Meta hash still written for steer targeting.
        pipe = await self._write("running", fenced=False)
        assert not any(c[0] in ("sadd", "srem") for c in pipe.calls)
        assert ("hset", "subagent:meta:t1:aaa111") in pipe.calls

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["completed", "cancelled"])
    async def test_terminal_removes_member(self, status):
        pipe = await self._write(status)
        assert ("srem", "subagent:active:t1", "aaa111") in pipe.calls
        assert not any(c[0] == "sadd" for c in pipe.calls)


@pytest.mark.asyncio
async def test_read_active_task_ids_none_without_redis():
    cache = MagicMock()
    cache.enabled = False
    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        assert await read_active_task_ids("t1") is None


@pytest.mark.asyncio
async def test_read_active_task_ids_decodes_and_sorts():
    cache = MagicMock()
    cache.enabled = True
    cache.client = MagicMock()
    cache.client.smembers = AsyncMock(return_value={b"zz9", b"aa1"})
    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        assert await read_active_task_ids("t1") == ["aa1", "zz9"]


# ---------------------------------------------------------------------------
# BackgroundTaskManager._resolve_active_tasks / get_live_task_info
# ---------------------------------------------------------------------------


def _make_btm() -> BackgroundTaskManager:
    with patch("src.server.services.background_task_manager.get_max_concurrent_workflows", return_value=10), \
         patch("src.server.services.background_task_manager.get_workflow_result_ttl", return_value=3600), \
         patch("src.server.services.background_task_manager.get_abandoned_workflow_timeout", return_value=3600), \
         patch("src.server.services.background_task_manager.get_cleanup_interval", return_value=60), \
         patch("src.server.services.background_task_manager.is_intermediate_storage_enabled", return_value=False), \
         patch("src.server.services.background_task_manager.get_max_stored_messages_per_agent", return_value=1000), \
         patch("src.server.services.background_task_manager.get_event_storage_backend", return_value="memory"), \
         patch("src.server.services.background_task_manager.get_redis_ttl_workflow_events", return_value=86400):
        return BackgroundTaskManager()


_REGISTRY_MOD = "ptc_agent.agent.middleware.background_subagent.registry"


def _live_task(task_id: str) -> SimpleNamespace:
    ato = MagicMock()
    ato.done = MagicMock(return_value=False)
    return SimpleNamespace(task_id=task_id, completed=False, asyncio_task=ato)


def _hydrated_placeholder(task_id: str) -> SimpleNamespace:
    # Shape produced by checkpoint hydration of a task running on another
    # worker: pending-looking but with no writer coroutine here.
    return SimpleNamespace(task_id=task_id, completed=False, asyncio_task=None)


def _patch_local(tasks):
    tasks = [_live_task(t) if isinstance(t, str) else t for t in tasks]
    registry = MagicMock()
    registry.get_all_tasks = AsyncMock(return_value=tasks)
    store = MagicMock()
    store.get_registry = AsyncMock(return_value=registry if tasks else None)
    return patch(
        "src.server.services.background_registry_store."
        "BackgroundRegistryStore.get_instance",
        return_value=store,
    )


class TestResolveActiveTasks:
    @pytest.mark.asyncio
    async def test_remote_members_filtered_by_lock_never_evicted(self):
        # Dead members are filtered from the answer but left in the set:
        # read-path SREM races a resume re-adding the same task id.
        btm = _make_btm()
        probe = AsyncMock(return_value={"live11"})
        with (
            _patch_local([]),
            patch(
                f"{_REGISTRY_MOD}.read_active_task_ids",
                new=AsyncMock(return_value=["dead99", "live11"]),
            ),
            patch(
                "src.server.services.writer_guard.held_task_namespaces",
                new=probe,
            ),
        ):
            assert await btm._resolve_active_tasks("t1") == ["live11"]
        probe.assert_awaited_once_with("t1", ["dead99", "live11"])

    @pytest.mark.asyncio
    async def test_local_live_tasks_bypass_the_probe(self):
        # A writer coroutine running in this process is known-alive — listed
        # even if its Redis publication failed (member missing) or unfenced,
        # and never probed.
        btm = _make_btm()
        probe = AsyncMock(side_effect=AssertionError("must not probe local"))
        with (
            _patch_local(["loc1"]),
            patch(
                f"{_REGISTRY_MOD}.read_active_task_ids",
                new=AsyncMock(return_value=["loc1"]),
            ),
            patch(
                "src.server.services.writer_guard.held_task_namespaces",
                new=probe,
            ),
        ):
            assert await btm._resolve_active_tasks("t1") == ["loc1"]

    @pytest.mark.asyncio
    async def test_union_of_local_and_verified_remote(self):
        btm = _make_btm()
        with (
            _patch_local(["loc1"]),
            patch(
                f"{_REGISTRY_MOD}.read_active_task_ids",
                new=AsyncMock(return_value=["rem2", "dead3"]),
            ),
            patch(
                "src.server.services.writer_guard.held_task_namespaces",
                new=AsyncMock(return_value={"rem2"}),
            ),
        ):
            assert await btm._resolve_active_tasks("t1") == ["loc1", "rem2"]

    @pytest.mark.asyncio
    async def test_probe_failure_keeps_members(self):
        btm = _make_btm()
        with (
            _patch_local([]),
            patch(
                f"{_REGISTRY_MOD}.read_active_task_ids",
                new=AsyncMock(return_value=["aaa111"]),
            ),
            patch(
                "src.server.services.writer_guard.held_task_namespaces",
                new=AsyncMock(return_value=None),
            ),
        ):
            assert await btm._resolve_active_tasks("t1") == ["aaa111"]

    @pytest.mark.asyncio
    async def test_redis_unknown_falls_back_to_local_registry(self):
        btm = _make_btm()
        with (
            _patch_local(["loc1"]),
            patch(
                f"{_REGISTRY_MOD}.read_active_task_ids",
                new=AsyncMock(return_value=None),
            ),
        ):
            assert await btm._resolve_active_tasks("t1") == ["loc1"]

    @pytest.mark.asyncio
    async def test_hydrated_placeholder_vanishes_once_owner_settles(self):
        # A peer worker hydrates a running task (e.g. to steer it), leaving
        # a pending-shaped placeholder with no writer coroutine. When the
        # owner completes (SREM + lock release), the placeholder must not
        # keep the task listed as active.
        btm = _make_btm()
        with (
            _patch_local([_hydrated_placeholder("abc123")]),
            patch(
                f"{_REGISTRY_MOD}.read_active_task_ids",
                new=AsyncMock(return_value=[]),
            ),
        ):
            assert await btm._resolve_active_tasks("t1") == []

    @pytest.mark.asyncio
    async def test_hydrated_placeholder_listed_via_lock_while_owner_lives(self):
        btm = _make_btm()
        probe = AsyncMock(return_value={"abc123"})
        with (
            _patch_local([_hydrated_placeholder("abc123")]),
            patch(
                f"{_REGISTRY_MOD}.read_active_task_ids",
                new=AsyncMock(return_value=["abc123"]),
            ),
            patch(
                "src.server.services.writer_guard.held_task_namespaces",
                new=probe,
            ),
        ):
            assert await btm._resolve_active_tasks("t1") == ["abc123"]
        probe.assert_awaited_once_with("t1", ["abc123"])


@pytest.mark.asyncio
async def test_get_live_task_info_reports_tasks_without_local_record():
    """Tail mode on a peer worker: no in-process record, tasks still listed."""
    btm = _make_btm()
    with patch.object(
        btm, "_resolve_active_tasks", new=AsyncMock(return_value=["v2YdrQx"])
    ):
        info = await btm.get_live_task_info("t1")
    assert info == {"live": False, "run_id": None, "active_tasks": ["v2YdrQx"]}
