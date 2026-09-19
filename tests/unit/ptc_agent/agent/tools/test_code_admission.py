"""Per-computer admission for sandbox code runs.

The contract under test is the one the multi-worker checklist asks for: the slot
is a Redis-only signal, so every way it can go wrong has to end in the code
running unbounded rather than in a stuck turn.
"""

import asyncio
import time
from types import SimpleNamespace

import pytest

from ptc_agent.agent.tools import code_admission
from ptc_agent.agent.tools.code_admission import (
    CODE_SLOTS_BY_TIER,
    code_run_key,
    code_run_slot,
    code_slots_for_tier,
    reap_horizon_seconds,
)


class _FakePipeline:
    """The four commands ``acquire_slot_member`` issues, queued then executed."""

    def __init__(self, redis):
        self._redis = redis
        self._ops = []

    def zremrangebyscore(self, key, lo, hi):
        self._ops.append(("zremrangebyscore", key, lo, hi))
        return self

    def zadd(self, key, mapping):
        self._ops.append(("zadd", key, mapping))
        return self

    def zcard(self, key):
        self._ops.append(("zcard", key))
        return self

    def expire(self, key, ttl):
        self._ops.append(("expire", key, ttl))
        return self

    async def execute(self):
        results = []
        for op in self._ops:
            if op[0] == "zremrangebyscore":
                _, key, _lo, hi = op
                members = self._redis.zsets.setdefault(key, {})
                removed = [m for m, s in members.items() if s <= hi]
                for m in removed:
                    del members[m]
                results.append(len(removed))
            elif op[0] == "zadd":
                _, key, mapping = op
                self._redis.zsets.setdefault(key, {}).update(mapping)
                results.append(len(mapping))
            elif op[0] == "zcard":
                results.append(len(self._redis.zsets.get(op[1], {})))
            else:
                results.append(True)
        return results


class FakeRedis:
    def __init__(self, fail=False):
        self.zsets: dict[str, dict[str, float]] = {}
        self.fail = fail

    def pipeline(self):
        if self.fail:
            raise ConnectionError("redis is down")
        return _FakePipeline(self)

    async def zrem(self, key, member):
        return int(self.zsets.get(key, {}).pop(member, None) is not None)


class _FakeCache:
    def __init__(self, client, enabled=True):
        self.client = client
        self.enabled = enabled


@pytest.fixture
def redis(monkeypatch):
    client = FakeRedis()
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: _FakeCache(client)
    )
    # The once-per-process fail-open warning must not leak between tests.
    monkeypatch.setattr(code_admission, "_fail_open_logged", False)
    return client


def _holders(redis, computer_id="comp-1"):
    return redis.zsets.get(code_run_key(computer_id), {})


# ---------------------------------------------------------------------------
# Sizing
# ---------------------------------------------------------------------------

class TestSizing:
    def test_each_tier_gets_its_vcpu_count(self):
        assert CODE_SLOTS_BY_TIER == {"standard": 1, "performance": 2, "max": 4}
        assert code_slots_for_tier("performance") == 2
        assert code_slots_for_tier("MAX") == 4

    def test_an_unknown_tier_gets_the_most_restrictive_entry(self):
        """A tier this build has never heard of is a row from another colour;
        over-serialising one machine is cheaper than over-subscribing it."""
        assert code_slots_for_tier(None) == 1
        assert code_slots_for_tier("titanic") == 1

    def test_the_reap_horizon_outlasts_the_longest_legitimate_hold(self):
        # Three attempts of max_execution_time plus transport, plus margin.
        assert reap_horizon_seconds(300) > 3 * 300

    def test_the_reap_horizon_is_overridable_for_tests(self, monkeypatch):
        monkeypatch.setenv("CODE_RUN_SLOT_TTL", "7")
        assert reap_horizon_seconds(300) == 7


# ---------------------------------------------------------------------------
# Admission
# ---------------------------------------------------------------------------

class TestAdmission:
    @pytest.mark.asyncio
    async def test_a_run_holds_one_member_and_releases_it(self, redis):
        async with code_run_slot("comp-1", tier="standard") as slot_id:
            assert slot_id is not None
            assert list(_holders(redis)) == [slot_id]
        assert _holders(redis) == {}

    @pytest.mark.asyncio
    async def test_the_slot_is_released_when_the_body_raises(self, redis):
        with pytest.raises(RuntimeError):
            async with code_run_slot("comp-1", tier="standard"):
                raise RuntimeError("boom")
        assert _holders(redis) == {}

    @pytest.mark.asyncio
    async def test_a_second_run_waits_while_the_first_holds_the_only_slot(
        self, redis
    ):
        observed: list[int] = []
        released = asyncio.Event()

        async def first():
            async with code_run_slot("comp-1", tier="standard"):
                observed.append(len(_holders(redis)))
                await asyncio.sleep(0.2)
            released.set()

        async def second():
            await asyncio.sleep(0.02)
            assert not released.is_set()
            async with code_run_slot("comp-1", tier="standard") as slot_id:
                # The first run is out before the second is ever admitted.
                assert released.is_set()
                assert list(_holders(redis)) == [slot_id]
                observed.append(len(_holders(redis)))

        await asyncio.gather(first(), second())
        assert observed == [1, 1]
        assert _holders(redis) == {}

    @pytest.mark.asyncio
    async def test_a_performance_machine_admits_two_at_once(self, redis):
        entered = asyncio.Event()

        async def run(hold: float):
            async with code_run_slot("comp-1", tier="performance"):
                entered.set()
                await asyncio.sleep(hold)
                return len(_holders(redis))

        counts = await asyncio.gather(run(0.1), run(0.1))
        assert entered.is_set()
        assert max(counts) == 2

    @pytest.mark.asyncio
    async def test_a_leaked_member_is_reaped_by_the_horizon(self, redis, monkeypatch):
        """A worker that died mid-run leaves its member behind; the next
        admission past the horizon reaps it rather than blocking forever."""
        monkeypatch.setenv("CODE_RUN_SLOT_TTL", "5")
        redis.zsets[code_run_key("comp-1")] = {"dead-worker": time.time() - 60}

        async with code_run_slot("comp-1", tier="standard") as slot_id:
            assert slot_id is not None
            assert list(_holders(redis)) == [slot_id]


# ---------------------------------------------------------------------------
# Fail-open
# ---------------------------------------------------------------------------

class TestFailOpen:
    @pytest.mark.asyncio
    async def test_no_computer_never_touches_redis(self, redis):
        async with code_run_slot(None, tier="standard") as slot_id:
            assert slot_id is None
        assert redis.zsets == {}

    @pytest.mark.asyncio
    async def test_a_disabled_cache_runs_unbounded(self, monkeypatch):
        monkeypatch.setattr(code_admission, "_fail_open_logged", False)
        monkeypatch.setattr(
            "src.utils.cache.redis_cache.get_cache_client",
            lambda: _FakeCache(FakeRedis(), enabled=False),
        )
        async with code_run_slot("comp-1", tier="standard") as slot_id:
            assert slot_id is None

    @pytest.mark.asyncio
    async def test_a_redis_error_runs_unbounded(self, monkeypatch):
        monkeypatch.setattr(code_admission, "_fail_open_logged", False)
        monkeypatch.setattr(
            "src.utils.cache.redis_cache.get_cache_client",
            lambda: _FakeCache(FakeRedis(fail=True)),
        )
        async with code_run_slot("comp-1", tier="standard") as slot_id:
            assert slot_id is None

    @pytest.mark.asyncio
    async def test_the_fail_open_warning_is_logged_once_per_process(
        self, monkeypatch
    ):
        monkeypatch.setattr(code_admission, "_fail_open_logged", False)
        monkeypatch.setattr(
            "src.utils.cache.redis_cache.get_cache_client",
            lambda: _FakeCache(FakeRedis(fail=True)),
        )
        warnings = []
        monkeypatch.setattr(
            code_admission.logger, "warning", lambda *a, **k: warnings.append(a)
        )
        for _ in range(3):
            async with code_run_slot("comp-1", tier="standard"):
                pass
        assert len(warnings) == 1

    @pytest.mark.asyncio
    async def test_a_queue_wait_that_expires_runs_unbounded_rather_than_failing(
        self, redis, monkeypatch
    ):
        monkeypatch.setenv("CODE_RUN_QUEUE_TIMEOUT", "0")
        redis.zsets[code_run_key("comp-1")] = {"someone-else": time.time()}

        async with code_run_slot("comp-1", tier="standard") as slot_id:
            assert slot_id is None
        # The other holder's member is untouched: a fail-open run holds nothing
        # and so can never release a slot it did not take.
        assert list(_holders(redis)) == ["someone-else"]

    @pytest.mark.asyncio
    async def test_a_release_failure_does_not_break_the_run(
        self, redis, monkeypatch
    ):
        async with code_run_slot("comp-1", tier="standard"):
            async def boom(*_a, **_k):
                raise ConnectionError("redis went away mid-run")

            monkeypatch.setattr(redis, "zrem", boom)


# ---------------------------------------------------------------------------
# The tool
# ---------------------------------------------------------------------------

class TestExecuteCodeTool:
    """``ExecuteCode`` is where the bound actually binds."""

    @pytest.mark.asyncio
    async def test_a_session_bound_run_holds_a_slot_for_its_machine(self, redis):
        from ptc_agent.agent.tools.code_execution import create_execute_code_tool

        seen: list = []

        class _Backend:
            sandbox = SimpleNamespace(
                config=SimpleNamespace(
                    security=SimpleNamespace(max_execution_time=300)
                )
            )

            async def aexecute_code(self, code, thread_id=None):
                seen.append(dict(_holders(redis)))
                return SimpleNamespace(
                    success=True, stdout="ok", stderr="", mcp_trace=[]
                )

        session = SimpleNamespace(computer_id="comp-1", resource_tier="standard")
        tool = create_execute_code_tool(_Backend(), None, session=session)

        out = await tool.ainvoke({"code": "print(1)"})

        assert out.startswith("SUCCESS")
        assert len(seen) == 1 and len(seen[0]) == 1
        assert _holders(redis) == {}

    @pytest.mark.asyncio
    async def test_a_run_with_no_session_skips_admission(self, redis):
        from ptc_agent.agent.tools.code_execution import create_execute_code_tool

        class _Backend:
            sandbox = None

            async def aexecute_code(self, code, thread_id=None):
                return SimpleNamespace(
                    success=True, stdout="ok", stderr="", mcp_trace=[]
                )

        tool = create_execute_code_tool(_Backend(), None)
        out = await tool.ainvoke({"code": "print(1)"})

        assert out.startswith("SUCCESS")
        assert redis.zsets == {}
