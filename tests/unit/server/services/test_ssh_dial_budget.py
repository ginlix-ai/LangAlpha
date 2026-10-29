"""The hourly budget of SSH dials to addresses a caller names."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from ptc_agent.config.core import SSHConfig
from src.server.services import ssh_dial_budget
from src.server.services.ssh_dial_budget import SSHDialBudgetExceeded, spend_dials

USER = "user-ssh-budget"


class _Redis:
    def __init__(self) -> None:
        self.values: dict[str, int] = {}
        self.ttls: dict[str, int] = {}
        self.fail = False

    async def incrby(self, key, n):
        if self.fail:
            raise ConnectionError("redis down")
        self.values[key] = self.values.get(key, 0) + n
        return self.values[key]

    async def expire(self, key, ttl):
        self.ttls[key] = ttl
        return True

    async def decrby(self, key, n):
        self.values[key] -= n
        return self.values[key]

    def pipeline(self, transaction=True):
        client = self

        class _Pipe:
            def __init__(self):
                self.ops = []

            def __getattr__(self, name):
                def queue(*args):
                    self.ops.append((name, args))

                return queue

            async def execute(self):
                return [await getattr(client, n)(*a) for n, a in self.ops]

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

        return _Pipe()


@pytest.fixture
def redis(monkeypatch):
    client = _Redis()
    cache = SimpleNamespace(enabled=True, client=client)
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: cache)
    return client


@pytest.fixture
def clock(monkeypatch):
    now = SimpleNamespace(t=1_700_000_000.0)
    monkeypatch.setattr(ssh_dial_budget.time, "time", lambda: now.t)
    return now


def _settings(limit):
    return SSHConfig(dials_per_hour=limit)


@pytest.mark.asyncio
async def test_spending_within_the_budget_passes(redis, clock):
    await spend_dials(USER, 6, settings=_settings(10))
    await spend_dials(USER, 4, settings=_settings(10))


@pytest.mark.asyncio
async def test_the_attempt_past_the_budget_is_refused_whole_and_given_back(redis, clock):
    await spend_dials(USER, 8, settings=_settings(10))

    with pytest.raises(SSHDialBudgetExceeded) as refused:
        await spend_dials(USER, 3, settings=_settings(10))

    assert 0 < refused.value.retry_after <= 3600
    # What the refusal took is returned, so a request that still fits passes.
    await spend_dials(USER, 2, settings=_settings(10))


@pytest.mark.asyncio
async def test_zero_attempts_cost_nothing_and_zero_limit_is_unlimited(redis, clock):
    await spend_dials(USER, 0, settings=_settings(1))
    await spend_dials(USER, 10_000, settings=_settings(0))
    assert redis.values == {}


@pytest.mark.asyncio
async def test_redis_down_fails_open(redis, clock):
    redis.fail = True
    await spend_dials(USER, 500, settings=_settings(1))


@pytest.mark.asyncio
async def test_no_cache_fails_open(monkeypatch):
    cache = SimpleNamespace(enabled=False, client=None)
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: cache)
    await spend_dials(USER, 500, settings=_settings(1))


@pytest.mark.asyncio
async def test_the_window_resets_and_users_are_apart(redis, clock):
    await spend_dials(USER, 5, settings=_settings(5))
    await spend_dials("someone-else", 5, settings=_settings(5))
    with pytest.raises(SSHDialBudgetExceeded):
        await spend_dials(USER, 1, settings=_settings(5))

    clock.t += 3600
    await spend_dials(USER, 5, settings=_settings(5))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "left,phrase",
    [(30, "in 1 minute."), (60, "in 1 minute."), (61, "in 2 minutes."), (1800, "in 30 minutes.")],
)
async def test_the_wait_is_worded_in_minutes(redis, clock, left, phrase):
    clock.t = (clock.t // 3600) * 3600 + 3600 - left
    await spend_dials(USER, 1, settings=_settings(1))
    with pytest.raises(SSHDialBudgetExceeded) as refused:
        await spend_dials(USER, 1, settings=_settings(1))
    assert str(refused.value).endswith(phrase)
    assert str(refused.value).startswith(
        "This account has tried too many SSH connections in the last hour. Try again "
    )


# What a create spends: the dials plan_create makes against the machine.
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fields,spent",
    [
        ({}, [2]),
        ({"root_dir": "/home/dev/langalpha"}, [1]),
        ({"tunnel_id": "5b0f3c1e-8d2a-4c6b-9e7f-1a2b3c4d5e6f"}, []),
    ],
    ids=["default-folder", "named-folder", "tunnel"],
)
async def test_a_create_spends_its_dials_before_the_first(monkeypatch, fields, spent):
    from unittest.mock import AsyncMock

    from src.server.models.computer import ComputerSSHSpec
    from src.server.services import ssh_computers

    recorded: list[int] = []

    async def record(user_id, attempts, *, settings):
        recorded.append(attempts)

    class Stop(Exception):
        pass

    monkeypatch.setattr(ssh_computers, "spend_dials", record)
    monkeypatch.setattr(ssh_computers, "check_ssh_capacity", AsyncMock())
    monkeypatch.setattr(ssh_computers, "has_client_key", AsyncMock(return_value=True))
    monkeypatch.setattr(ssh_computers, "tunnels_enabled", lambda: True)
    monkeypatch.setattr(ssh_computers, "get_user_tunnel", AsyncMock(return_value={}))
    monkeypatch.setattr(ssh_computers, "client_key", AsyncMock(side_effect=Stop))
    spec = ComputerSSHSpec(
        host="203.0.113.10",
        username="dev",
        host_key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl",
        **fields,
    )

    with pytest.raises(Stop):
        await ssh_computers.plan_create(USER, spec, settings=SSHConfig())

    assert recorded == spent
