"""The Redis scripts that keep what a computer's ``la`` asks the user.

The gate's unit tests fake the store's API, so only here does the shipped
Lua run: one request per command however many ask at once, an answer recorded
once and only while it waits, an approval taken once, a "no" kept readable
for the send that waits on it, a look that never writes, a read that raced a
writer told to ask again, the cap, and a request that expired unanswered
reported once.

Requires a real Redis (``REDIS_URL``); no Postgres.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio

from src.config.settings import get_cli_approval_approved_ttl
from src.server.services import cli_approvals as service

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

COMPUTER = "c0000000-0000-4000-8000-000000000001"
OTHER_COMPUTER = "c0000000-0000-4000-8000-000000000002"
SUMMARY = {
    "target": {"id": "t", "name": "Notes"},
    "details": {"kind": "thread"},
    "requester": {"computer_id": COMPUTER},
}


@pytest_asyncio.fixture(loop_scope="session")
async def redis(monkeypatch):
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    if not redis_url.startswith("redis://"):
        pytest.skip("REDIS_URL not set to a real Redis instance")

    from src.utils.cache.redis_cache import RedisCacheClient

    client = RedisCacheClient(url=redis_url, max_connections=10)
    try:
        await client.connect()
    except Exception as exc:
        pytest.skip(f"Redis is not reachable at REDIS_URL: {exc}")
    if not client.enabled or not client.client:
        pytest.skip("Redis client did not initialize")
    monkeypatch.setattr(
        service, "get_cache_client", lambda: SimpleNamespace(enabled=True, client=client.client)
    )
    yield client.client
    await client.disconnect()


@pytest_asyncio.fixture(loop_scope="session")
async def user(redis):
    """A fresh user, so its keys are its own; they go with it."""
    user_id = f"approvals-it-{uuid.uuid4()}"
    yield user_id
    keys = [k async for k in redis.scan_iter(match=f"cliapproval:{{{user_id}}}:*")]
    if keys:
        await redis.delete(*keys)


@pytest.fixture
def announced(monkeypatch) -> list[tuple[str, str, str]]:
    told: list[tuple[str, str, str]] = []

    async def announce(user_id, approval_id, status):
        told.append((user_id, approval_id, status))

    monkeypatch.setattr(service, "announce", announce)
    return told


def _ask(
    user_id: str,
    digest: str = "d1",
    *,
    computer: str = COMPUTER,
    named: str | None = None,
    summary: dict | None = SUMMARY,
    ttl: timedelta = timedelta(minutes=30),
    max_pending: int = 5,
):
    """Asks and writes a request when there is none; ``summary=None`` only looks."""
    return service.ask(
        user_id,
        computer,
        digest,
        action="thread_delete",
        named=named,
        summary=summary,
        ttl=ttl,
        max_pending=max_pending,
    )


async def _create(user_id: str, digest: str = "d1", **kwargs) -> str:
    found = await _ask(user_id, digest, **kwargs)
    assert found.outcome == service.CREATED
    assert found.approval_id
    return found.approval_id


async def _keys(redis, user_id: str) -> set[str]:
    return {
        k.decode() if isinstance(k, bytes) else k
        async for k in redis.scan_iter(match=f"cliapproval:{{{user_id}}}:*")
    }


async def test_many_asking_the_same_command_at_once_write_one_request(user, redis):
    answers = await asyncio.gather(*(_ask(user) for _ in range(8)))

    # A caller whose read raced the writer is told to ask again.
    retried = [
        await _ask(user) if found.outcome == service.MOVED else found for found in answers
    ]

    assert sum(f.outcome == service.CREATED for f in answers) == 1
    assert {f.outcome for f in answers} <= {service.CREATED, service.PENDING, service.MOVED}
    assert all(f.outcome in (service.CREATED, service.PENDING) for f in retried)
    assert len({f.approval_id for f in retried}) == 1
    assert await redis.zcard(service.pending_key(user)) == 1


async def test_the_keys_share_one_slot_and_carry_their_ttls(user, redis):
    approval_id = await _create(user)

    keys = [
        service.row_key(user, approval_id),
        service.intent_key(user, COMPUTER, "d1"),
        service.pending_key(user),
    ]
    assert {k.split("{", 1)[1].split("}", 1)[0] for k in keys} == {user}
    ttls = [await redis.pttl(k) for k in keys]
    assert all(28 * 60_000 < t <= 30 * 60_000 for t in ttls[:2])
    assert ttls[2] > 30 * 60_000


async def test_one_answer_is_recorded_and_the_rest_refused(user, announced):
    approval_id = await _create(user)

    answers = await asyncio.gather(
        *(service.decide(user, approval_id, approve=n % 2 == 0) for n in range(6)),
        return_exceptions=True,
    )

    assert sum(isinstance(a, dict) for a in answers) == 1
    assert all(isinstance(a, service.AlreadyDecided) for a in answers if not isinstance(a, dict))
    assert await service.pending(user) == []


async def test_an_approval_is_taken_once_and_holds_for_its_own_ttl(user, redis, announced):
    approval_id = await _create(user)
    await service.decide(user, approval_id, approve=True)
    ttl = await redis.pttl(service.row_key(user, approval_id))
    approved_ms = int(get_cli_approval_approved_ttl().total_seconds() * 1000)

    taken = await asyncio.gather(*(_ask(user, summary=None) for _ in range(6)))

    assert approved_ms - 5_000 < ttl <= approved_ms
    assert [f.approval_id for f in taken if f.outcome == service.TAKEN] == [approval_id]
    # Whoever read the intent before it went is told to ask again; the rest
    # read none and find none.
    assert {f.outcome for f in taken} <= {service.TAKEN, service.ABSENT, service.MOVED}
    assert (await service.get(user, approval_id)).status == service.USED
    assert await redis.exists(service.intent_key(user, COMPUTER, "d1")) == 0
    again = await _ask(user)
    assert again.outcome == service.CREATED and again.approval_id != approval_id


async def test_a_no_is_kept_and_answered_only_to_the_request_that_waits_on_it(
    user, redis, announced
):
    approval_id = await _create(user)
    await service.decide(user, approval_id, approve=False)

    assert (await service.get(user, approval_id)).status == service.REJECTED
    assert 9 * 60_000 < await redis.pttl(service.row_key(user, approval_id)) <= 10 * 60_000
    assert await redis.exists(service.intent_key(user, COMPUTER, "d1")) == 0

    refused = await _ask(user, named=approval_id, summary=None)
    assert (refused.outcome, refused.approval_id) == (service.REJECTED, approval_id)
    # The "no" belongs to that command on that computer only.
    other_digest = await _ask(user, "d2", named=approval_id, summary=None)
    other_computer = await _ask(user, computer=OTHER_COMPUTER, named=approval_id, summary=None)
    assert other_digest.outcome == service.ABSENT
    assert other_computer.outcome == service.ABSENT
    # A send that waits on nothing asks anew.
    again = await _ask(user)
    assert again.outcome == service.CREATED and again.approval_id != approval_id


async def test_naming_a_request_that_is_not_live_never_writes(user, redis, announced):
    used = await _create(user, "u")
    await service.decide(user, used, approve=True)
    assert (await _ask(user, "u", summary=None)).outcome == service.TAKEN
    lapsed = str(uuid.uuid4())
    before = await _keys(redis, user)
    members = await redis.zrange(service.pending_key(user), 0, -1)

    for named in (used, lapsed):
        found = await _ask(user, "u", named=named, summary=None)
        assert (found.outcome, found.approval_id) == (service.ABSENT, None)
    # Nor does a look with no name.
    assert (await _ask(user, "u", summary=None)).outcome == service.ABSENT

    assert await _keys(redis, user) == before
    assert await redis.zrange(service.pending_key(user), 0, -1) == members
    assert await redis.exists(service.intent_key(user, COMPUTER, "u")) == 0


async def test_a_command_with_a_live_request_answers_pending_whatever_is_named(user):
    approval_id = await _create(user)

    for named in (None, str(uuid.uuid4()), approval_id):
        for summary in (None, SUMMARY):
            found = await _ask(user, named=named, summary=summary)
            assert (found.outcome, found.approval_id) == (service.PENDING, approval_id)


async def test_a_read_that_raced_a_writer_is_told_to_ask_again_and_writes_nothing(user, redis):
    # What the store read before the script ran no longer matches: here the
    # intent is empty while the read saw an id.
    stale = str(uuid.uuid4())
    new_id = str(uuid.uuid4())
    new_key = service.row_key(user, new_id)

    answer = await redis.eval(
        service.ASK,
        5,
        service.intent_key(user, COMPUTER, "d1"),
        service.pending_key(user),
        service.row_key(user, stale),
        new_key,
        new_key,
        0, 60_000, new_id, stale, "", COMPUTER, "d1", 5, 600_000, 600_000,
        "thread_delete", '{"target": {}, "details": {}, "requester": {}}',
    )  # fmt: skip

    assert [a.decode() if isinstance(a, bytes) else a for a in answer] == [service.MOVED]
    assert await _keys(redis, user) == set()


async def test_the_cap_counts_only_requests_still_waiting(user, announced):
    first = await _create(user, "a", max_pending=2)
    await _create(user, "b", max_pending=2)

    full = await _ask(user, "c", max_pending=2)
    assert (full.outcome, full.approval_id) == (service.LIMIT, None)
    await service.decide(user, first, approve=False)
    freed = await _ask(user, "c", max_pending=2)

    assert freed.outcome == service.CREATED


async def test_a_request_that_expires_unanswered_fails_closed_and_is_reported_once(
    user, announced
):
    approval_id = await _create(user, ttl=timedelta(milliseconds=300))
    await asyncio.sleep(0.5)

    assert await service.get(user, approval_id) is None
    with pytest.raises(service.Unknown):
        await service.decide(user, approval_id, approve=True)
    assert await service.pending(user) == []
    assert await service.pending(user) == []
    assert announced == [(user, approval_id, service.EXPIRED)]
    again = await _ask(user)
    assert again.outcome == service.CREATED and again.approval_id != approval_id


async def test_the_list_is_oldest_first_and_drops_what_was_evicted(user, redis):
    first = await _create(user, "a")
    await asyncio.sleep(0.005)
    second = await _create(user, "b")

    listed = await service.pending(user)

    assert [a.approval_id for a in listed] == [first, second]
    assert listed[0].to_wire()["target"] == {"id": "t", "name": "Notes"}

    # A row evicted from under the set must not hold a place under the cap.
    await redis.delete(service.row_key(user, first))
    listed = await service.pending(user)

    assert [a.approval_id for a in listed] == [second]
    assert await redis.zscore(service.pending_key(user), first) is None
    assert await redis.zscore(service.pending_key(user), second) is not None


async def test_the_list_leaves_out_a_request_answered_between_its_reads(user, redis):
    approval_id = await _create(user)
    # The set still names it, as when an answer lands between the set's read
    # and its row's.
    await redis.hset(service.row_key(user, approval_id), "status", "approved")

    assert await service.pending(user) == []


async def test_a_shorter_request_never_shortens_the_list_an_earlier_one_is_in(user, redis):
    await _create(user, "long", ttl=timedelta(minutes=30))
    await _create(user, "short", ttl=timedelta(minutes=1))

    left = await redis.pttl(service.pending_key(user))

    assert left > timedelta(minutes=30).total_seconds() * 1000
