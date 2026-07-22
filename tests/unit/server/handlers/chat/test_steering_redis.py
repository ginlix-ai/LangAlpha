"""Redis-level contract for the steer / reclaim primitives.

``wait_or_steer``'s accept-after-exit reclaim rests entirely on two Redis
facts that the higher-level tests only ever mock: ``steer_thread`` returns the
*exact* payload string it queued, and ``unsteer_thread`` removes that same
string by an exact-match ``LREM`` (True iff it was still there). These tests
exercise both against a stateful fake Redis so a wrong key, a wrong LREM count,
or a broken truthiness mapping fails here instead of silently in production.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from unittest.mock import AsyncMock, patch

import pytest

from .redis_fakes import FakeCache

CACHE = "src.utils.cache.redis_cache.get_cache_client"
KEY = "workflow:steering:t-1"
SR_DB = "src.server.database.runs.subagent_runs"


@contextlib.contextmanager
def _ledger(*, active=None, chain=(), latest_statuses=None):
    """Patch the steering fallback's ledger reads."""
    with (
        patch(f"{SR_DB}.get_active_task_run", new=AsyncMock(return_value=active)),
        patch(f"{SR_DB}.list_task_runs", new=AsyncMock(return_value=list(chain))),
        patch(
            f"{SR_DB}.get_latest_run_statuses",
            new=AsyncMock(return_value=latest_statuses or {}),
        ),
    ):
        yield


@pytest.mark.asyncio
async def test_steer_thread_queues_and_returns_the_exact_payload(monkeypatch):
    from src.server.handlers.chat.steering import steer_thread

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)

    result = await steer_thread("t-1", "hello", "u-1")

    assert result is not None
    # The returned payload is byte-identical to what landed in the queue — the
    # reclaim's exact-match LREM depends on this identity.
    assert cache.client.lists[KEY] == [result["payload"]]
    assert result["position"] == 1
    body = json.loads(result["payload"])
    assert body["content"] == "hello" and body["user_id"] == "u-1"
    # An EXPIRE was issued so an unconsumed steer can't leak forever.
    assert KEY in cache.client.ttls


@pytest.mark.asyncio
async def test_unsteer_reclaims_the_just_queued_payload(monkeypatch):
    from src.server.handlers.chat.steering import steer_thread, unsteer_thread

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)

    result = await steer_thread("t-1", "hello", "u-1")
    reclaimed = await unsteer_thread("t-1", result["payload"])

    assert reclaimed is True
    assert cache.client.lists[KEY] == []


@pytest.mark.asyncio
async def test_unsteer_false_when_a_drain_consumed_it_first(monkeypatch):
    """Drain won the race (the payload is already gone): LREM removes 0, so the
    caller must report accepted, not route fresh."""
    from src.server.handlers.chat.steering import steer_thread, unsteer_thread

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)

    result = await steer_thread("t-1", "hello", "u-1")
    await cache.client.delete(KEY)  # simulate the exit drain's atomic wipe

    assert await unsteer_thread("t-1", result["payload"]) is False


@pytest.mark.asyncio
async def test_unsteer_only_removes_the_exact_payload(monkeypatch):
    """LREM is exact-match, not a blanket clear — a different steer left by
    another request must survive the reclaim."""
    from src.server.handlers.chat.steering import steer_thread, unsteer_thread

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)

    mine = await steer_thread("t-1", "mine", "u-1")
    other = await steer_thread("t-1", "other", "u-2")

    assert await unsteer_thread("t-1", mine["payload"]) is True
    assert cache.client.lists[KEY] == [other["payload"]]


@pytest.mark.asyncio
async def test_unsteer_false_when_cache_disabled(monkeypatch):
    from src.server.handlers.chat.steering import unsteer_thread

    cache = FakeCache()
    cache.enabled = False
    monkeypatch.setattr(CACHE, lambda: cache)

    assert await unsteer_thread("t-1", "whatever") is False


@pytest.mark.asyncio
async def test_unsteer_false_on_redis_error(monkeypatch):
    """A Redis fault on the LREM must degrade to False (report accepted), never
    raise into the streaming generator."""
    from src.server.handlers.chat.steering import unsteer_thread

    cache = FakeCache()

    async def _boom(*_args):
        raise ConnectionError("redis down")

    cache.client.lrem = _boom
    monkeypatch.setattr(CACHE, lambda: cache)

    assert await unsteer_thread("t-1", "whatever") is False


# ---------------------------------------------------------------------------
# steer_subagent cross-worker resolution (v4 2.4e)
# ---------------------------------------------------------------------------


def _seed_meta(
    cache, thread_id: str, task_id: str, status: str, task_run_id: str = ""
) -> None:
    cache.client.hashes[f"subagent:meta:{thread_id}:{task_id}"] = {
        "tool_call_id": "tc-remote",
        "status": status,
        "subagent_type": "research",
        "task_run_id": task_run_id,
    }


def _queued(cache, key: str) -> list[dict]:
    """Decoded steering payloads on a queue key."""
    return [json.loads(e) for e in cache.client.lists[key]]


@pytest.mark.asyncio
async def test_steer_subagent_resolves_foreign_task_via_meta(monkeypatch):
    """No local registry entry (task owned by another worker): the Redis
    task meta supplies the tool_call_id, and the follow-up lands on the
    steering list the remote writer consumes."""
    from src.server.handlers.chat.steering import steer_subagent

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)
    _seed_meta(cache, "t-foreign", "abc123", "running")

    result = await steer_subagent("t-foreign", "abc123", "go left", "u-1")

    assert result["success"] is True
    assert result["tool_call_id"] == "tc-remote"
    # Meta without a task_run_id (pre-ledger) → legacy task-lifetime key,
    # unfenced payload.
    key = "subagent:steering:tc-remote"
    (payload,) = _queued(cache, key)
    assert payload["content"] == "go left"
    assert payload["expected_task_run_id"] is None
    assert payload["input_id"] == result["input_id"]
    assert key in cache.client.ttls


@pytest.mark.asyncio
async def test_steer_subagent_meta_run_id_fences_the_queue(monkeypatch):
    """Meta carrying the execution's task_run_id routes the steer onto the
    run-scoped key and stamps the payload — a later resume of the same task
    can never consume it."""
    from src.server.handlers.chat.steering import steer_subagent

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)
    _seed_meta(cache, "t-fenced", "abc123", "running", task_run_id="run-9")

    result = await steer_subagent("t-fenced", "abc123", "go", "u-1")

    assert result["success"] is True
    key = "subagent:steering:tc-remote:run-9"
    (payload,) = _queued(cache, key)
    assert payload["content"] == "go"
    assert payload["expected_task_run_id"] == "run-9"
    assert payload["input_id"] == result["input_id"]
    assert key in cache.client.ttls


@pytest.mark.asyncio
async def test_steer_subagent_meta_terminal_is_409(monkeypatch):
    from fastapi import HTTPException

    from src.server.handlers.chat.steering import steer_subagent

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)
    _seed_meta(cache, "t-foreign2", "abc123", "completed")

    with pytest.raises(HTTPException) as exc:
        await steer_subagent("t-foreign2", "abc123", "go", "u-1")
    assert exc.value.status_code == 409


@pytest.mark.asyncio
async def test_steer_subagent_reclaims_when_run_settles_mid_push(monkeypatch):
    """Accept/sweep arbitration: the owner finalizes and sweeps between the
    sender's admission read and its RPUSH. The post-push recheck sees the
    terminal meta (written before the sweep), reclaims the entry with an
    exact-match LREM, and refuses — never a success ack for input nobody
    will ever read."""
    from fastapi import HTTPException

    from src.server.handlers.chat.steering import steer_subagent

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)

    running = {
        "tool_call_id": "tc-remote",
        "status": "running",
        "task_run_id": "run-9",
    }
    settled = dict(running, status="completed")
    with (
        patch(
            "ptc_agent.agent.middleware.background_subagent.redis_stream.read_task_meta",
            AsyncMock(side_effect=[running, settled]),
        ),
        pytest.raises(HTTPException) as exc,
    ):
        await steer_subagent("t-race", "abc123", "go", "u-1")

    assert exc.value.status_code == 409
    assert cache.client.lists.get("subagent:steering:tc-remote:run-9", []) == []


@pytest.mark.asyncio
async def test_steer_subagent_refuses_when_epoch_rotates_mid_push(monkeypatch):
    """R1 settles and R2 resumes the task between admission and push: the
    recheck's meta says "running" but for a different task_run_id, so the
    R1-scoped entry (which no sweep will ever visit again) is reclaimed."""
    from fastapi import HTTPException

    from src.server.handlers.chat.steering import steer_subagent

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)

    r1 = {
        "tool_call_id": "tc-remote",
        "status": "running",
        "task_run_id": "run-9",
    }
    r2 = dict(r1, task_run_id="run-10")
    with (
        patch(
            "ptc_agent.agent.middleware.background_subagent.redis_stream.read_task_meta",
            AsyncMock(side_effect=[r1, r2]),
        ),
        pytest.raises(HTTPException) as exc,
    ):
        await steer_subagent("t-race2", "abc123", "go", "u-1")

    assert exc.value.status_code == 409
    assert cache.client.lists.get("subagent:steering:tc-remote:run-9", []) == []


@pytest.mark.asyncio
async def test_steer_subagent_unknown_everywhere_is_404(monkeypatch):
    from fastapi import HTTPException

    from src.server.handlers.chat.steering import steer_subagent

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)

    with _ledger(), pytest.raises(HTTPException) as exc:
        await steer_subagent("t-nowhere", "zzz999", "go", "u-1")
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_steer_subagent_lapsed_meta_resolves_via_ledger(monkeypatch):
    """Meta gone (TTL/flush) but the ledger names a live run: routing
    identity comes from the chain's init launch call, the fence from the
    active slot — a provably-running task must not 404."""
    from src.server.handlers.chat.steering import steer_subagent

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)

    with _ledger(
        active={"task_run_id": "run-77", "task_id": "abc123"},
        chain=[
            {"cause": "init", "launch_tool_call_id": "tc-init"},
            {"cause": "resume", "launch_tool_call_id": "tc-resume"},
        ],
    ):
        result = await steer_subagent("t-lapsed", "abc123", "go", "u-1")

    assert result["success"] is True
    assert result["tool_call_id"] == "tc-init"
    key = "subagent:steering:tc-init:run-77"
    (payload,) = _queued(cache, key)
    assert payload["expected_task_run_id"] == "run-77"
    assert payload["input_id"] == result["input_id"]


@pytest.mark.asyncio
async def test_steer_subagent_lapsed_meta_terminal_chain_is_409(monkeypatch):
    from fastapi import HTTPException

    from src.server.handlers.chat.steering import steer_subagent

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)

    with (
        _ledger(latest_statuses={"abc123": "cancelled"}),
        pytest.raises(HTTPException) as exc,
    ):
        await steer_subagent("t-lapsed2", "abc123", "go", "u-1")
    assert exc.value.status_code == 409
    assert "cancelled" in exc.value.detail


@pytest.mark.asyncio
async def test_steer_subagent_lapsed_meta_unstamped_chain_is_404(monkeypatch):
    """A live run whose chain never recorded an init launch call has no
    routing identity — there is no queue key the writer would drain."""
    from fastapi import HTTPException

    from src.server.handlers.chat.steering import steer_subagent

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)

    with (
        _ledger(
            active={"task_run_id": "run-77", "task_id": "abc123"},
            chain=[{"cause": "init", "launch_tool_call_id": None}],
        ),
        pytest.raises(HTTPException) as exc,
    ):
        await steer_subagent("t-lapsed3", "abc123", "go", "u-1")
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_steer_subagent_local_task_fast_path(monkeypatch):
    """A locally-owned live task resolves from the registry (no meta needed)
    and keys the steering list by its tool_call_id."""
    from src.server.handlers.chat.steering import steer_subagent
    from src.server.services.background_registry_store import (
        BackgroundRegistryStore,
    )

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)

    store = BackgroundRegistryStore.get_instance()
    registry = await store.get_or_create_registry("t-steer-local")
    task = await registry.register(
        tool_call_id="tc-local",
        description="d",
        prompt="p",
        subagent_type="general-purpose",
    )
    # A local WRITER handle is what makes the registry entry authoritative;
    # without one the entry is a hydrated shadow and meta is consulted.
    task.asyncio_task = asyncio.create_task(asyncio.sleep(30))
    try:
        result = await steer_subagent(
            "t-steer-local", task.task_id, "hello", "u-1"
        )
    finally:
        task.asyncio_task.cancel()
        store._registries.pop("t-steer-local", None)

    assert result["success"] is True
    assert result["tool_call_id"] == "tc-local"
    # register() without a ledger run id → legacy key, unfenced payload.
    (payload,) = _queued(cache, "subagent:steering:tc-local")
    assert payload["content"] == "hello"
    assert payload["expected_task_run_id"] is None


@pytest.mark.asyncio
async def test_done_local_handle_defers_to_fresh_running_meta(monkeypatch):
    """A settled local task keeps its DONE handle until collector cleanup —
    meanwhile a later turn on another worker may have resumed the task.
    Local authority requires a LIVE writer: a done handle is history, so the
    fresh 'running' meta (not a stale local 409) must route the steer to the
    list the real writer consumes."""
    from src.server.handlers.chat.steering import steer_subagent
    from src.server.services.background_registry_store import (
        BackgroundRegistryStore,
    )

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)

    store = BackgroundRegistryStore.get_instance()
    registry = await store.get_or_create_registry("t-steer-done")
    task = await registry.register(
        tool_call_id="tc-old-local",
        description="d",
        prompt="p",
        subagent_type="general-purpose",
    )
    done = asyncio.create_task(asyncio.sleep(0))
    await done
    task.asyncio_task = done  # settled locally...
    task.completed = True
    _seed_meta(cache, "t-steer-done", task.task_id, "running")  # ...but live elsewhere

    try:
        result = await steer_subagent("t-steer-done", task.task_id, "go", "u-1")
    finally:
        store._registries.pop("t-steer-done", None)

    assert result["success"] is True
    assert result["tool_call_id"] == "tc-remote"  # meta identity, not tc-old-local
    (payload,) = _queued(cache, "subagent:steering:tc-remote")
    assert payload["content"] == "go"


@pytest.mark.asyncio
async def test_stale_hydrated_shadow_defers_to_fresh_terminal_meta(monkeypatch):
    """A registry entry with no local writer handle is a hydrated shadow of
    another worker's task; it never updates when the real writer settles, so
    the fresh terminal meta — not the forever-pending shadow — must answer.
    (Trusting the shadow would 200 the steer into a dead task's list.)"""
    from fastapi import HTTPException

    from src.server.handlers.chat.steering import steer_subagent
    from src.server.services.background_registry_store import (
        BackgroundRegistryStore,
    )

    cache = FakeCache()
    monkeypatch.setattr(CACHE, lambda: cache)

    store = BackgroundRegistryStore.get_instance()
    registry = await store.get_or_create_registry("t-steer-shadow")
    task = await registry.register(
        tool_call_id="tc-shadow",
        description="d",
        prompt="p",
        subagent_type="general-purpose",
    )
    assert task.asyncio_task is None and not task.completed  # shadow shape
    _seed_meta(cache, "t-steer-shadow", task.task_id, "completed")

    try:
        with pytest.raises(HTTPException) as exc:
            await steer_subagent("t-steer-shadow", task.task_id, "hello", "u-1")
    finally:
        store._registries.pop("t-steer-shadow", None)

    assert exc.value.status_code == 409
    assert "completed" in exc.value.detail
    assert "subagent:steering:tc-shadow" not in cache.client.lists
