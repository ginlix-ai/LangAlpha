"""A turn's late word about a computer's token, kept on Redis itself: the
compare and the write are one Lua step, so an answer kept after the turn was
built is never overwritten or dropped by it."""

import os
import time
from dataclasses import asdict
from unittest.mock import patch

import pytest
import pytest_asyncio

from src.server.services.claude_code import machine
from src.server.services.claude_code.machine import MachineAnswer, answer_key, keep_unless_newer, read_answer

pytestmark = pytest.mark.integration

CID = "0b9e6a7c-3f0e-4c47-9a53-2f4b1d1c8e21"


@pytest_asyncio.fixture
async def cache():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    if not redis_url.startswith("redis://"):
        pytest.skip("REDIS_URL not set to a real Redis instance")
    from src.utils.cache.redis_cache import RedisCacheClient

    client = RedisCacheClient(url=redis_url, max_connections=2)
    try:
        await client.connect()
    except Exception as exc:
        pytest.skip(f"Redis is not reachable at REDIS_URL: {exc}")
    if not client.enabled or not client.client:
        pytest.skip("Redis client did not initialize")
    await client.delete(answer_key(CID))
    with patch.object(machine, "get_cache_client", return_value=client):
        yield client
    await client.delete(answer_key(CID))
    await client.disconnect()


async def _kept_at(cache, file: str, checked_at: float) -> None:
    await cache.set(answer_key(CID), asdict(MachineAnswer(file, "a@example.com", None, checked_at)), ttl=60)


@pytest.mark.asyncio
async def test_a_word_from_before_the_answer_kept_never_lands(cache):
    built_at = time.time()
    await _kept_at(cache, "works", built_at + 1)
    await keep_unless_newer(CID, MachineAnswer("refused"), built_at)
    await keep_unless_newer(CID, None, built_at)
    kept = await read_answer(CID)
    assert (kept.file, kept.checked_at, kept.email) == ("works", built_at + 1, "a@example.com")


@pytest.mark.asyncio
async def test_a_word_from_after_the_answer_kept_lands(cache):
    built_at = time.time()
    await _kept_at(cache, "works", built_at - 1)
    await keep_unless_newer(CID, MachineAnswer("refused"), built_at)
    kept = await read_answer(CID)
    assert kept.file == "refused" and kept.checked_at > built_at
    assert 0 < await cache.ttl(answer_key(CID)) <= machine.KEPT_TTL_S
    await keep_unless_newer(CID, None, kept.checked_at)
    assert await read_answer(CID) is None


@pytest.mark.asyncio
async def test_a_word_lands_where_nothing_or_no_answer_is_kept(cache):
    built_at = time.time()
    await keep_unless_newer(CID, MachineAnswer("refused"), built_at)
    assert (await read_answer(CID)).file == "refused"
    await cache.set(answer_key(CID), "not an answer", ttl=60)
    await keep_unless_newer(CID, MachineAnswer("unknown"), built_at)
    assert (await read_answer(CID)).file == "unknown"
