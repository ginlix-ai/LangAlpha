"""A suspended account: read from the org block, carried through the cache
and the last-known stand-in like the organization, and refused as itself."""

from __future__ import annotations

import asyncio
import contextvars
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi import HTTPException

from src.config.models import PolicyLayer
from src.server.dependencies import usage_limits as limits
from src.server.services import membership
from src.server.services import account
from src.server.services.account import ACCOUNT_SUSPENDED, AccountSuspended, Standing
from src.server.services.account_contract import (
    NO_ORG,
    Account,
    MalformedAccount,
    Org,
    parse_account,
)
from src.server.services.policy import is_policy_refusal, merge
from tests.unit.server.services.test_account_contract import (
    _Auth,
    _Cache,
    _deployment,
)

SUSPENDED = {
    "valid": True,
    "access_tier": 2,
    "plan_display_name": "Pro",
    "org": {"id": "o-1", "name": "Acme", "suspended": True},
    "features": {"teams": True},
    "policy": {"trading": False},
}
MESSAGE = (
    "This account's access to Acme has ended. Ask an admin of your "
    "organization if this is unexpected."
)


# --- parse ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("block", "suspended"),
    [
        ({"id": "o", "name": "Acme"}, False),
        ({"id": "o", "name": "Acme", "suspended": False}, False),
        ({"id": "o", "name": "Acme", "suspended": True}, True),
    ],
)
def test_suspended_is_read_and_absent_means_not(block, suspended):
    parsed = parse_account({"access_tier": 1, "org": block})
    assert parsed.org == Org(id="o", name="Acme", suspended=suspended)
    assert parsed.suspended is suspended


@pytest.mark.parametrize("value", ["true", 1, None, [], {}])
def test_a_suspended_that_is_not_a_boolean_is_malformed(value):
    with pytest.raises(MalformedAccount):
        parse_account({"access_tier": 1, "org": {"id": "o", "name": "A", "suspended": value}})


@pytest.mark.asyncio
async def test_a_malformed_suspended_makes_the_account_unknown():
    bad = {**SUSPENDED, "org": {"id": "o-1", "name": "Acme", "suspended": "yes"}}
    with _deployment(_Cache(), _Auth(bad), door=True):
        assert await account.account_for("u") is None


def test_no_org_and_an_unknown_account_are_not_suspended():
    assert NO_ORG.suspended is False
    assert account.Standing(None, None).refusal() is None
    assert account.Standing(NO_ORG, NO_ORG).refusal() is None


# --- the door, the cache and the stand-in ---------------------------------------


@pytest.mark.asyncio
async def test_a_suspended_account_is_carried_through_the_cache():
    cache, auth = _Cache(), _Auth(SUSPENDED)
    with _deployment(cache, auth, door=True):
        first = await account.account_for("u")
        again = await account.account_for("u")
    assert first.suspended and again.suspended
    assert len(auth.requests) == 1  # the second read is the cached entry


@pytest.mark.asyncio
async def test_it_stands_in_during_an_outage_like_the_organization():
    cache = _Cache()
    with _deployment(cache, _Auth(SUSPENDED), door=True):
        await account.account_for("u")
    cache.store.pop("platform_membership:v4:u")  # the 5-minute entry lapsed
    with _deployment(cache, _Auth(httpx.ConnectError("down")), door=True):
        assert (await account.account_for("u")).suspended


@pytest.mark.asyncio
async def test_a_record_from_before_a_change_notice_reads_as_unknown():
    cache = _Cache()
    with _deployment(cache, _Auth(SUSPENDED), door=True):
        await account.account_for("u")
        await membership.forget_membership("u")
    # Reinstated, perhaps: the record says what the account was, not what it is.
    with _deployment(cache, _Auth(httpx.ConnectError("down")), door=True):
        assert await account.account_for("u") is None


@pytest.mark.asyncio
async def test_suspension_holds_whatever_the_organization_features_say():
    no_teams = {**SUSPENDED, "features": {"teams": False}}
    with _deployment(_Cache(), _Auth(no_teams), door=True):
        found = await account.account_for("u")
    assert found.suspended and found.org.name == "Acme"
    # Not suspended, the organization is dropped at the door as before.
    active = {**no_teams, "org": {"id": "o-1", "name": "Acme"}}
    with _deployment(_Cache(), _Auth(active), door=True):
        assert await account.account_for("u") is NO_ORG


@pytest.mark.asyncio
async def test_with_the_door_shut_nothing_is_read():
    cache, auth = _Cache(), _Auth(SUSPENDED)
    with _deployment(cache, auth, door=False):
        (await account.read_standing("u")).refuse_if_suspended()
        await account.admit("u")
        assert (await account.read_standing("u", unknown_on_cache_error=True)).suspended is False
    assert cache.calls == [] and auth.requests == []


# --- the refusal ------------------------------------------------------------------


def _suspended() -> Account:
    return Account(org=Org("o-1", "Acme", suspended=True), features={"teams": True})


def test_the_refusal_is_its_own():
    suspended = _suspended()
    refusal = account.Standing(suspended, suspended).refusal()
    assert isinstance(refusal, AccountSuspended)
    assert refusal.status_code == 403
    assert refusal.detail == {
        "message": MESSAGE, "type": ACCOUNT_SUSPENDED, "organization": "Acme",
    }
    assert str(refusal) == MESSAGE
    # Never worked around where a policy refusal is not.
    assert is_policy_refusal(refusal)


@pytest.mark.asyncio
async def test_work_under_the_policy_is_refused_and_the_policy_still_reads():
    suspended = Account(
        org=Org("o-1", "Acme", suspended=True), features={"teams": True},
        policy=PolicyLayer.model_validate({"public_links": True}),
    )
    with patch(f"{account.__name__}.read_standing", AsyncMock(return_value=Standing(suspended, suspended))):
        with pytest.raises(AccountSuspended):
            await account.require_policy("u")
        with pytest.raises(AccountSuspended):
            await account.own_access("u")
        # Read for what it allows rather than to work under it, it answers.
        assert await account.policy_for("u") == merge(None, suspended.policy)
        assert await account.serves_public_links("u") is True


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["daytona", "e2b", "docker", "ssh"])
async def test_no_computer_is_taken_up_for_a_suspended_owner(kind):
    from src.server.services.account import refuse_machine_use

    suspended = _suspended()
    with (
        patch("src.server.services.membership.teams_door_open", return_value=True),
        patch(f"{account.__name__}.read_standing", AsyncMock(return_value=Standing(suspended, suspended))),
        pytest.raises(AccountSuspended),
    ):
        await refuse_machine_use(kind, "u")


@pytest.mark.asyncio
async def test_a_provided_computer_still_works_for_an_unknown_owner():
    from src.server.services.account import refuse_machine_use

    with (
        patch("src.server.services.membership.teams_door_open", return_value=True),
        patch(f"{account.__name__}.read_standing", AsyncMock(return_value=Standing(None, None))),
    ):
        await refuse_machine_use("daytona", "u")
        with pytest.raises(HTTPException) as unknown:
            await refuse_machine_use("ssh", "u")
    assert unknown.value.status_code == 503


@pytest.mark.asyncio
async def test_a_provided_computer_reads_nothing_with_the_flag_off():
    from src.server.services.account import refuse_machine_use

    cache, auth = _Cache(), _Auth(SUSPENDED)
    with _deployment(cache, auth, door=False):
        await refuse_machine_use("daytona", "u")
    assert cache.calls == [] and auth.requests == []


# --- one read a request ------------------------------------------------------------


def _mgets(cache) -> int:
    return len([c for c in cache.calls if c[0] == "mget"])


@pytest.mark.asyncio
async def test_a_request_shares_one_read_for_its_own_lifetime():
    cache, auth = _Cache(), _Auth({**SUSPENDED, "org": {"id": "o-1", "name": "Acme"}})
    with _deployment(cache, auth, door=True):
        with membership.membership_read_scope():
            await account.admit("u")  # the gate, first
            await account.account_for("u")  # a route's own reads after it
            await membership.access_tier("u")
            assert _mgets(cache) == 1

            # Another user in the same request is not the shared one.
            await account.account_for("v")
            assert _mgets(cache) == 2

            # A task the request starts shares its context, so it would
            # share the read too; it may not hold it for longer than a request.
            task_context = contextvars.copy_context()
        assert _mgets(cache) == 2
        # The request has ended: nothing it started reuses its answer.
        await asyncio.create_task(account.account_for("u"), context=task_context)
        assert _mgets(cache) == 3
        # Outside a request every read asks.
        await account.account_for("u")
        assert _mgets(cache) == 4


@pytest.mark.asyncio
async def test_an_answer_older_than_its_age_limit_is_read_again():
    cache, auth = _Cache(), _Auth({"valid": True, "access_tier": 2})
    clock = [1000.0]
    with _deployment(cache, auth, door=True), patch(
        f"{limits.__name__}.time.monotonic", lambda: clock[0]
    ):
        # A stream or socket that lives as long as its request.
        with membership.membership_read_scope():
            await account.admit("u")
            await account.account_for("u")
            assert _mgets(cache) == 1
            clock[0] += membership._SHARED_READ_MAX_AGE + 0.1
            await account.account_for("u")
            assert _mgets(cache) == 2


@pytest.mark.asyncio
async def test_concurrent_misses_ask_the_auth_service_once():
    cache = _Cache()
    gate = asyncio.Event()

    class _Slow(_Auth):
        async def post(self, url, json=None, headers=None):
            await gate.wait()
            return await super().post(url, json=json, headers=headers)

    auth = _Slow({"valid": True, "access_tier": 2})
    with _deployment(cache, auth, door=True):
        reads = [asyncio.create_task(account.account_for("u")) for _ in range(5)]
        await asyncio.sleep(0)
        gate.set()
        await asyncio.gather(*reads)
    assert len(auth.requests) == 1


@pytest.mark.asyncio
async def test_cancelling_the_first_reader_leaves_the_shared_ask_for_the_others():
    cache = _Cache()
    gate = asyncio.Event()

    class _Slow(_Auth):
        async def post(self, url, json=None, headers=None):
            await gate.wait()
            return await super().post(url, json=json, headers=headers)

    auth = _Slow({"valid": True, "access_tier": 2, "plan_display_name": "Pro"})
    with _deployment(cache, auth, door=True):
        first = asyncio.create_task(membership.access_tier("u"))
        await asyncio.sleep(0)
        others = [asyncio.create_task(membership.access_tier("u")) for _ in range(3)]
        await asyncio.sleep(0)
        # The request that started the ask goes away (a client hanging up).
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        gate.set()
        assert await asyncio.gather(*others) == [2, 2, 2]
    assert len(auth.requests) == 1


class _Down(_Cache):
    """Redis refusing every read."""

    async def mget(self, keys, strict=False):
        self.calls.append(("mget", list(keys), strict))
        if strict:
            raise ConnectionError("redis down")
        return [None] * len(keys)


@pytest.mark.asyncio
async def test_a_redis_error_lets_the_gate_pass_and_asks_nobody():
    cache, auth = _Down(), _Auth({**SUSPENDED, "org": {"id": "o-1", "name": "Acme"}})
    with _deployment(cache, auth, door=True):
        # An unknown account keeps its reads, on a route that read no
        # membership before the gate existed.
        with membership.membership_read_scope():
            await account.admit("u")
        assert (await account.read_standing("u", unknown_on_cache_error=True)).suspended is False
    assert auth.requests == []


@pytest.mark.asyncio
async def test_a_redis_error_on_a_read_older_than_the_gate_asks_the_auth_service_as_before():
    cache, auth = _Down(), _Auth({"valid": True, "access_tier": 2, "plan_display_name": "Pro"})
    with _deployment(cache, auth, door=True):
        with membership.membership_read_scope():
            await account.admit("u")
            # The gate's unknown is not shared: the plan gate after it asks.
            assert await membership.access_tier("u") == 2
        assert len(auth.requests) == 1
        # Concurrent misses share one ask.
        await asyncio.gather(*(membership.access_tier("v") for _ in range(4)))
    assert len(auth.requests) == 2


@pytest.mark.asyncio
async def test_a_redis_error_with_the_flag_off_reads_as_a_miss_as_before():
    cache, auth = _Down(), _Auth({"valid": True, "access_tier": 2})
    with _deployment(cache, auth, door=False):
        await account.admit("u")
        await membership.access_tier("u")
    assert [c[2] for c in cache.calls if c[0] == "mget"] == [False]
    assert len(auth.requests) == 1


@pytest.mark.asyncio
async def test_a_refresh_asks_again_and_is_what_later_reads_get():
    cache = _Cache()
    plan_changed = {"valid": True, "access_tier": 3, "plan_display_name": "Max"}
    auth = _Auth({"valid": True, "access_tier": 2}, plan_changed)
    with _deployment(cache, auth, door=True):
        await account.admit("u")
        await membership.read_membership("u", refresh=True)
        assert (await membership.read_membership("u"))["access_tier"] == 3
    assert len(auth.requests) == 2
