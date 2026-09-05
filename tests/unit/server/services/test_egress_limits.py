"""Per-grant rate + concurrency limits for the egress relay.

Limits are protective plumbing rather than the security boundary, so the two
contracts worth pinning are: they bind per grant (one saturated connector never
starves another), and every Redis failure path yields instead of taking the
relay down. The concurrency half holds a member in the shared ZSET slot guard,
so a request that dies without releasing is reaped by score rather than leaking
a slot forever.
"""

from __future__ import annotations

from contextlib import AsyncExitStack
from types import SimpleNamespace

import pytest

from src.server.services.egress import limits as limits_mod
from src.server.services.egress.limits import (
    CONCURRENCY_LIMIT,
    RATE_LIMIT_RPM,
    RelayLimited,
    acquire_slot,
)
from src.server.utils import slot_guard

GRANT_A = "grant-egress-a"
GRANT_B = "grant-egress-b"

# Two deliberately tiny budgets so boundary arms stay cheap, each applied by a
# fixture that isolates a single dimension (a low rpm would otherwise trip
# first in the concurrency arms). The shipped budgets get their own test.
TIGHT_RPM = 2
NARROW_CONCURRENCY = 2


class _FakeRedis:
    """Only the commands acquire_slot issues: the rate counter's incr/expire
    and the concurrency ZSET's zremrangebyscore/zadd/zcard/expire/zrem."""

    def __init__(self) -> None:
        self.values: dict[str, int] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.ttls: dict[str, int] = {}
        self.fail: set[str] = set()
        self.calls: list[tuple[str, str]] = []

    def _guard(self, command: str, key: str) -> None:
        self.calls.append((command, key))
        if command in self.fail:
            raise ConnectionError(f"redis {command} unavailable")

    async def incr(self, key: str) -> int:
        self._guard("incr", key)
        self.values[key] = self.values.get(key, 0) + 1
        return self.values[key]

    async def expire(self, key: str, ttl: int) -> bool:
        self._guard("expire", key)
        self.ttls[key] = ttl
        return True

    async def zremrangebyscore(self, key: str, low: str, high: float) -> int:
        self._guard("zremrangebyscore", key)
        members = self.zsets.setdefault(key, {})
        stale = [m for m, score in members.items() if score <= float(high)]
        for member in stale:
            del members[member]
        return len(stale)

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        self._guard("zadd", key)
        self.zsets.setdefault(key, {}).update(mapping)
        return len(mapping)

    async def zcard(self, key: str) -> int:
        self._guard("zcard", key)
        return len(self.zsets.get(key, {}))

    async def zrem(self, key: str, member: str) -> int:
        self._guard("zrem", key)
        return 1 if self.zsets.get(key, {}).pop(member, None) is not None else 0

    def pipeline(self, *_args, **_kwargs):
        """Both pipeline shapes the callers use: an async context manager and a
        bare queue. Batching does not change WHICH commands are sent, and that
        is what these tests assert, so the queue replays onto this client."""
        client = self

        class _Pipe:
            def __init__(self) -> None:
                self._ops: list = []

            def __getattr__(self, name):
                def _queue(*args, **kwargs):
                    self._ops.append((name, args, kwargs))
                    return self

                return _queue

            async def execute(self) -> list:
                out = []
                for name, args, kwargs in self._ops:
                    out.append(await getattr(client, name)(*args, **kwargs))
                self._ops.clear()
                return out

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_exc):
                return False

        return _Pipe()


@pytest.fixture
def redis(monkeypatch):
    client = _FakeRedis()
    cache = SimpleNamespace(enabled=True, client=client)
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: cache
    )
    return client


@pytest.fixture
def tight_rate(monkeypatch):
    """A 2/minute budget, with concurrency left generous so rate trips first."""
    monkeypatch.setattr(limits_mod, "RATE_LIMIT_RPM", TIGHT_RPM)
    monkeypatch.setattr(limits_mod, "CONCURRENCY_LIMIT", 8)


@pytest.fixture
def narrow_concurrency(monkeypatch):
    """A 2-slot cap, with rpm left high so concurrency trips first."""
    monkeypatch.setattr(limits_mod, "RATE_LIMIT_RPM", 1000)
    monkeypatch.setattr(limits_mod, "CONCURRENCY_LIMIT", NARROW_CONCURRENCY)


@pytest.fixture
def frozen_clock(monkeypatch):
    """Pin the minute bucket and the slot scores so a test controls both."""
    clock = SimpleNamespace(now=1_700_000_000.0)
    stub = SimpleNamespace(time=lambda: clock.now)
    monkeypatch.setattr(limits_mod, "time", stub)
    monkeypatch.setattr(slot_guard, "time", stub)
    return clock


def _conc_key(grant_id: str) -> str:
    return f"egress:conc:{grant_id}"


def _held(redis: _FakeRedis, grant_id: str) -> int:
    return len(redis.zsets.get(_conc_key(grant_id), {}))


# ---------------------------------------------------------------------------
# Rate
# ---------------------------------------------------------------------------


class TestRateLimit:
    @pytest.mark.asyncio
    async def test_allows_up_to_the_budget_then_denies(self, redis, frozen_clock, tight_rate):
        for _ in range(TIGHT_RPM):
            async with acquire_slot(GRANT_A):
                pass

        with pytest.raises(RelayLimited) as excinfo:
            async with acquire_slot(GRANT_A):
                pass
        assert excinfo.value.kind == "rate"

    @pytest.mark.asyncio
    async def test_the_shipped_budget_is_the_one_enforced(self, redis, frozen_clock):
        """No fixture override: the constants the relay actually runs with."""
        for _ in range(RATE_LIMIT_RPM):
            async with acquire_slot(GRANT_A):
                pass

        with pytest.raises(RelayLimited) as excinfo:
            async with acquire_slot(GRANT_A):
                pass
        assert excinfo.value.kind == "rate"

    @pytest.mark.asyncio
    async def test_a_denied_request_takes_no_concurrency_slot(
        self, redis, frozen_clock, tight_rate
    ):
        for _ in range(TIGHT_RPM):
            async with acquire_slot(GRANT_A):
                pass
        assert _held(redis, GRANT_A) == 0

        with pytest.raises(RelayLimited):
            async with acquire_slot(GRANT_A):
                pass
        assert _held(redis, GRANT_A) == 0

    @pytest.mark.asyncio
    async def test_budget_resets_on_the_next_minute_bucket(self, redis, frozen_clock, tight_rate):
        for _ in range(TIGHT_RPM):
            async with acquire_slot(GRANT_A):
                pass
        with pytest.raises(RelayLimited):
            async with acquire_slot(GRANT_A):
                pass

        frozen_clock.now += 60
        async with acquire_slot(GRANT_A):
            pass

        rate_keys = {k for k in redis.values if k.startswith("egress:rate:")}
        assert len(rate_keys) == 2

    @pytest.mark.asyncio
    async def test_counters_carry_a_ttl(self, redis, frozen_clock):
        async with acquire_slot(GRANT_A):
            pass

        assert set(redis.ttls) == {
            f"egress:rate:{GRANT_A}:{int(frozen_clock.now // 60)}",
            _conc_key(GRANT_A),
        }
        assert all(ttl > 0 for ttl in redis.ttls.values())


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


class TestConcurrencyLimit:
    @pytest.mark.asyncio
    async def test_holds_a_slot_for_the_body_and_releases_on_exit(
        self, redis, frozen_clock, narrow_concurrency
    ):
        async with acquire_slot(GRANT_A):
            assert _held(redis, GRANT_A) == 1
        assert _held(redis, GRANT_A) == 0

    @pytest.mark.asyncio
    async def test_the_shipped_cap_is_the_one_enforced(self, redis, frozen_clock):
        """No fixture override: the constants the relay actually runs with."""
        async with AsyncExitStack() as held:
            for _ in range(CONCURRENCY_LIMIT):
                await held.enter_async_context(acquire_slot(GRANT_A))

            with pytest.raises(RelayLimited) as excinfo:
                async with acquire_slot(GRANT_A):
                    pass
            assert excinfo.value.kind == "concurrency"

    @pytest.mark.asyncio
    async def test_over_limit_acquisition_is_denied(self, redis, frozen_clock, narrow_concurrency):
        cap = NARROW_CONCURRENCY
        async with AsyncExitStack() as held:
            for _ in range(cap):
                await held.enter_async_context(acquire_slot(GRANT_A))
            assert _held(redis, GRANT_A) == cap

            with pytest.raises(RelayLimited) as excinfo:
                async with acquire_slot(GRANT_A):
                    pass
            assert excinfo.value.kind == "concurrency"
            # The refused attempt takes its own member back out, so a burst of
            # denials cannot wedge the set above the cap forever.
            assert _held(redis, GRANT_A) == cap

    @pytest.mark.asyncio
    async def test_a_released_slot_frees_capacity(self, redis, frozen_clock, narrow_concurrency):
        cap = NARROW_CONCURRENCY
        async with AsyncExitStack() as held:
            for _ in range(cap - 1):
                await held.enter_async_context(acquire_slot(GRANT_A))

            async with acquire_slot(GRANT_A):
                with pytest.raises(RelayLimited):
                    async with acquire_slot(GRANT_A):
                        pass

            # The cap-th holder exited; the next caller fits again.
            async with acquire_slot(GRANT_A):
                assert _held(redis, GRANT_A) == cap

    @pytest.mark.asyncio
    async def test_slot_is_released_when_the_body_raises(self, redis, frozen_clock, narrow_concurrency):
        with pytest.raises(RuntimeError):
            async with acquire_slot(GRANT_A):
                raise RuntimeError("relayed request blew up")

        assert _held(redis, GRANT_A) == 0

    @pytest.mark.asyncio
    async def test_a_slot_whose_holder_died_is_reaped_by_age(
        self, redis, frozen_clock, narrow_concurrency
    ):
        # The pathology the ZSET exists for: a worker that dies mid-request
        # releases nothing, and the key stays alive because traffic keeps
        # touching it. Members past the stale window are dropped on the next
        # admission, so capacity comes back without operator intervention.
        redis.zsets[_conc_key(GRANT_A)] = {
            "dead-holder-1": frozen_clock.now,
            "dead-holder-2": frozen_clock.now,
        }
        with pytest.raises(RelayLimited):
            async with acquire_slot(GRANT_A):
                pass

        frozen_clock.now += limits_mod._CONC_STALE_AFTER + 1
        async with acquire_slot(GRANT_A):
            assert _held(redis, GRANT_A) == 1


# ---------------------------------------------------------------------------
# Per-grant keying
# ---------------------------------------------------------------------------


class TestPerGrantIsolation:
    @pytest.mark.asyncio
    async def test_a_saturated_grant_does_not_block_another(self, redis, frozen_clock, narrow_concurrency):
        cap = NARROW_CONCURRENCY
        async with AsyncExitStack() as held:
            for _ in range(cap):
                await held.enter_async_context(acquire_slot(GRANT_A))
            with pytest.raises(RelayLimited):
                async with acquire_slot(GRANT_A):
                    pass

            async with acquire_slot(GRANT_B):
                assert _held(redis, GRANT_B) == 1

    @pytest.mark.asyncio
    async def test_rate_budgets_are_counted_per_grant(self, redis, frozen_clock, tight_rate):
        for _ in range(TIGHT_RPM):
            async with acquire_slot(GRANT_A):
                pass
        with pytest.raises(RelayLimited):
            async with acquire_slot(GRANT_A):
                pass

        async with acquire_slot(GRANT_B):
            pass

        minute = int(frozen_clock.now // 60)
        assert redis.values[f"egress:rate:{GRANT_B}:{minute}"] == 1


# ---------------------------------------------------------------------------
# Fail-open
# ---------------------------------------------------------------------------


class TestFailsOpen:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "cache",
        [
            SimpleNamespace(enabled=False, client=object()),
            SimpleNamespace(enabled=True, client=None),
        ],
        ids=["cache-disabled", "no-client"],
    )
    async def test_unavailable_redis_yields(self, monkeypatch, cache):
        monkeypatch.setattr(
            "src.utils.cache.redis_cache.get_cache_client", lambda: cache
        )
        entered = False
        async with acquire_slot(GRANT_A):
            entered = True
        assert entered is True

    @pytest.mark.asyncio
    async def test_failed_rate_check_yields(self, redis, frozen_clock):
        redis.fail.add("incr")

        entered = False
        async with acquire_slot(GRANT_A):
            entered = True

        assert entered is True
        assert ("zrem", _conc_key(GRANT_A)) not in redis.calls

    @pytest.mark.asyncio
    async def test_failed_concurrency_check_yields(self, redis, frozen_clock):
        # Regression: Redis dying BETWEEN the rate round trip and the
        # concurrency one used to escape acquire_slot as a raw exception
        # (a relay 500), while the docstring promises fail-open for both.
        redis.fail.add("zadd")

        entered = False
        async with acquire_slot(GRANT_A):
            entered = True

        assert entered is True
        # Nothing was acquired, so nothing must be released.
        assert ("zrem", _conc_key(GRANT_A)) not in redis.calls

    @pytest.mark.asyncio
    async def test_failed_release_does_not_surface_to_the_caller(
        self, redis, frozen_clock
    ):
        async with acquire_slot(GRANT_A):
            redis.fail.add("zrem")

        assert ("zrem", _conc_key(GRANT_A)) in redis.calls


# ---------------------------------------------------------------------------
# Rejection shape
# ---------------------------------------------------------------------------


class TestRejectionShape:
    @pytest.mark.parametrize(
        "kind,code", [("rate", "limited_rate"), ("concurrency", "limited_concurrency")]
    )
    def test_a_budget_rejection_is_a_relay_rejection(self, kind, code):
        """The route maps every refusal through one arm, so a limit must carry
        its own status, code and backoff hint rather than a bespoke branch."""
        from src.server.services.egress import RelayRejection

        limited = RelayLimited(kind)
        assert isinstance(limited, RelayRejection)
        assert (limited.status, limited.code, limited.retry_after) == (429, code, 5)
        assert kind in limited.detail
