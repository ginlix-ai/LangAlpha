"""Stream retention sweep: scope, oracle handling, lease and cursor contracts."""

from __future__ import annotations

import fnmatch
import uuid
from unittest.mock import patch

import pytest

from src.server.services import stream_retention_sweep as sweep_mod
from src.server.services.stream_retention_sweep import (
    StreamRetentionSweeper,
    _CURSOR_KEY,
    _LEASE_KEY,
    _canonical_uuid,
)
from tests.unit.redis_mock_pipeline import attach_pipeline


TID = "11111111-1111-4111-8111-111111111111"
LIVE_RUN = "22222222-2222-4222-8222-222222222222"
DEAD_RUN = "33333333-3333-4333-8333-333333333333"
GONE_RUN = "44444444-4444-4444-8444-444444444444"
LIVE_TASK = "55555555-5555-4555-8555-555555555555"
DEAD_TASK = "66666666-6666-4666-8666-666666666666"
V1_TASK = "Xk9_aB-cD3efGh"  # token_urlsafe, not a UUID

TTL = 86400


def root_key(run_id: str) -> str:
    return f"workflow:stream:{TID}:{run_id}"


def sub_key(task_id: str) -> str:
    return f"subagent:stream:{TID}:{task_id}"


class FakeRedis:
    """Enough of a Redis to exercise SCAN cursors, leases and EXPIRE NX."""

    def __init__(self, keys=()):
        self.keys = list(keys)
        self.strings: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.expire_calls: list[tuple[str, int, bool]] = []
        self.scan_calls: list[tuple[int, str]] = []
        self.lease_hijacked_by: str | None = None

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.strings:
            return None
        self.strings[key] = value
        return True

    async def get(self, key):
        if key == _LEASE_KEY and self.lease_hijacked_by is not None:
            return self.lease_hijacked_by
        return self.strings.get(key)

    async def delete(self, *keys):
        return sum(1 for k in keys if self.strings.pop(k, None) is not None)

    async def eval(self, script, numkeys, *args):
        """Only the compare-and-delete the lease release uses."""
        key, expected = args[0], args[1]
        if await self.get(key) == expected:
            return await self.delete(key)
        return 0

    async def scan(self, cursor=0, match=None, count=10):
        self.scan_calls.append((cursor, match))
        page = self.keys[cursor : cursor + count]
        nxt = cursor + count
        if nxt >= len(self.keys):
            nxt = 0
        if match:
            page = [k for k in page if fnmatch.fnmatchcase(k, match)]
        return nxt, page

    async def expire(self, key, ttl, nx=False):
        self.expire_calls.append((key, ttl, nx))
        if nx and key in self.ttls:
            return False
        self.ttls[key] = ttl
        return True


class FakeCache:
    def __init__(self, client, enabled=True):
        self.client = client
        self.enabled = enabled


async def _root_oracle(run_ids):
    known = {LIVE_RUN: "in_progress", DEAD_RUN: "completed"}
    return {r: known[r] for r in run_ids if r in known}


async def _task_oracle(task_ids):
    known = {LIVE_TASK: "in_progress", DEAD_TASK: "error"}
    return {t: known[t] for t in task_ids if t in known}


async def _boom(_ids):
    raise RuntimeError("ledger unavailable")


@pytest.fixture(autouse=True)
def _fresh_sweeper():
    StreamRetentionSweeper.reset_instance()
    yield
    StreamRetentionSweeper.reset_instance()


async def run_sweep(client, *, root=_root_oracle, task=_task_oracle, enabled=True):
    attach_pipeline(client)
    with (
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=FakeCache(client, enabled=enabled),
        ),
        patch(
            "src.config.settings.get_redis_ttl_workflow_events",
            return_value=TTL,
        ),
        patch("src.server.database.runs.lifecycle.get_run_statuses", root),
        patch("src.server.database.runs.subagent_runs.get_task_run_statuses", task),
    ):
        return await StreamRetentionSweeper().sweep_once()


# --------------------------------------------------------------- scope

@pytest.mark.parametrize(
    "value,expected",
    [
        (str(uuid.uuid4()), True),
        (TID, True),
        (V1_TASK, False),
        ("", False),
        ("not-a-uuid", False),
        # UUID() also accepts these spellings; no key builder emits them, so a
        # match here would mean we mis-read a v1 id as a task_run_id.
        ("{%s}" % TID, False),
        (f"urn:uuid:{TID}", False),
        (TID.replace("-", ""), False),
        (None, False),
    ],
)
def test_canonical_uuid_discriminates_v1_from_v2(value, expected):
    assert _canonical_uuid(value) is expected


@pytest.mark.asyncio
async def test_stamps_terminal_and_orphaned_root_streams():
    client = FakeRedis([root_key(DEAD_RUN), root_key(GONE_RUN)])
    tally = await run_sweep(client)
    assert set(client.ttls) == {root_key(DEAD_RUN), root_key(GONE_RUN)}
    assert tally["stamped"] == 2


@pytest.mark.asyncio
async def test_never_stamps_a_run_still_in_progress():
    client = FakeRedis([root_key(LIVE_RUN), sub_key(LIVE_TASK)])
    tally = await run_sweep(client)
    assert client.ttls == {}
    assert tally["stamped"] == 0


@pytest.mark.asyncio
async def test_v2_subagent_streams_use_the_task_run_ledger():
    client = FakeRedis([sub_key(DEAD_TASK), sub_key(LIVE_TASK)])
    await run_sweep(client)
    assert set(client.ttls) == {sub_key(DEAD_TASK)}


@pytest.mark.asyncio
async def test_v1_task_keyed_streams_are_skipped_and_counted():
    client = FakeRedis([sub_key(V1_TASK), root_key(DEAD_RUN)])
    tally = await run_sweep(client)
    assert sub_key(V1_TASK) not in client.ttls
    assert tally["skipped_v1"] == 1


@pytest.mark.asyncio
async def test_keys_of_an_unknown_shape_are_ignored():
    client = FakeRedis(
        [
            f"workflow:stream:{TID}",  # too few parts
            f"workflow:stream:{TID}:{DEAD_RUN}:extra",  # too many
            f"other:stream:{TID}:{DEAD_RUN}",  # not a stream namespace we own
        ]
    )
    tally = await run_sweep(client)
    assert client.ttls == {}
    assert tally["stamped"] == 0


@pytest.mark.asyncio
async def test_expire_is_always_nx():
    """The sweep is a retention floor; it must never lengthen a stamp."""
    client = FakeRedis([root_key(DEAD_RUN), sub_key(DEAD_TASK)])
    client.ttls[root_key(DEAD_RUN)] = 900  # already stamped by the collector
    await run_sweep(client)
    assert all(nx is True for _, _, nx in client.expire_calls)
    assert client.ttls[root_key(DEAD_RUN)] == 900


# -------------------------------------------------------------- oracle

@pytest.mark.asyncio
async def test_failed_root_oracle_stamps_nothing_from_that_batch():
    """A failed query must never read as 'every run is gone'."""
    client = FakeRedis([root_key(DEAD_RUN), root_key(GONE_RUN), sub_key(DEAD_TASK)])
    tally = await run_sweep(client, root=_boom)
    assert root_key(DEAD_RUN) not in client.ttls
    assert root_key(GONE_RUN) not in client.ttls
    assert set(client.ttls) == {sub_key(DEAD_TASK)}  # the v2 batch still lands
    assert tally["stamped"] == 1


@pytest.mark.asyncio
async def test_failed_task_oracle_stamps_nothing_from_that_batch():
    client = FakeRedis([sub_key(DEAD_TASK), root_key(DEAD_RUN)])
    await run_sweep(client, task=_boom)
    assert set(client.ttls) == {root_key(DEAD_RUN)}


# ------------------------------------------------------- lease + cursor

@pytest.mark.asyncio
async def test_a_held_lease_makes_the_cycle_a_noop():
    client = FakeRedis([root_key(DEAD_RUN)])
    client.strings[_LEASE_KEY] = "another-worker"
    tally = await run_sweep(client)
    assert tally == {}
    assert client.scan_calls == []
    assert client.strings[_LEASE_KEY] == "another-worker"  # not stolen


@pytest.mark.asyncio
async def test_lease_is_released_after_a_pass():
    client = FakeRedis([root_key(DEAD_RUN)])
    await run_sweep(client)
    assert _LEASE_KEY not in client.strings


@pytest.mark.asyncio
async def test_an_overrun_pass_does_not_release_someone_elses_lease():
    client = FakeRedis([root_key(DEAD_RUN)])
    client.lease_hijacked_by = "the-next-worker"
    await run_sweep(client)
    assert _LEASE_KEY in client.strings


@pytest.mark.asyncio
async def test_cursor_resumes_from_redis_and_is_persisted():
    """The next cycle is likely a different worker, so the cursor lives in Redis."""
    keys = [root_key(GONE_RUN), root_key(DEAD_RUN)]
    client = FakeRedis(keys)
    client.strings[_CURSOR_KEY] = "1"
    with patch.object(sweep_mod, "SCAN_BATCH", 1), patch.object(
        sweep_mod, "KEYS_PER_CYCLE", 1
    ):
        await run_sweep(client)
    # Resumed at index 1: the key at index 0 was never visited this cycle.
    assert set(client.ttls) == {root_key(DEAD_RUN)}
    assert client.strings[_CURSOR_KEY] == "0"


@pytest.mark.asyncio
async def test_a_cycle_is_bounded_and_hands_its_cursor_to_the_next_one():
    client = FakeRedis([root_key(GONE_RUN), root_key(DEAD_RUN)])
    with patch.object(sweep_mod, "SCAN_BATCH", 1), patch.object(
        sweep_mod, "KEYS_PER_CYCLE", 1
    ):
        tally = await run_sweep(client)
    assert tally["scanned"] == 1
    assert client.strings[_CURSOR_KEY] == "1"


# ------------------------------------------------------------- plumbing

@pytest.mark.asyncio
async def test_a_disabled_cache_is_a_noop():
    client = FakeRedis([root_key(DEAD_RUN)])
    tally = await run_sweep(client, enabled=False)
    assert tally == {}
    assert client.scan_calls == []


@pytest.mark.asyncio
async def test_legacy_meta_hashes_are_expired():
    """Written by workers on the previous release, read by nobody."""
    client = FakeRedis(
        [
            f"workflow:events:meta:{TID}:{DEAD_RUN}",
            f"subagent:events:meta:{TID}:{DEAD_TASK}",
        ]
    )
    tally = await run_sweep(client)
    assert tally["legacy"] == 2
    assert all(nx is True for _, _, nx in client.expire_calls)


@pytest.mark.asyncio
async def test_stop_event_halts_paging_mid_cycle():
    client = FakeRedis([root_key(GONE_RUN), root_key(DEAD_RUN)])
    sweeper = StreamRetentionSweeper()
    sweeper._stop_event.set()
    attach_pipeline(client)
    with (
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=FakeCache(client),
        ),
        patch("src.config.settings.get_redis_ttl_workflow_events", return_value=TTL),
        patch("src.server.database.runs.lifecycle.get_run_statuses", _root_oracle),
        patch(
            "src.server.database.runs.subagent_runs.get_task_run_statuses",
            _task_oracle,
        ),
    ):
        tally = await sweeper.sweep_once()
    assert client.scan_calls == []
    assert tally["scanned"] == 0
