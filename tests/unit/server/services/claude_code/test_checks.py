"""The kept check both token homes share: one vocabulary, read the same from
any worker, older workers' values included, and a hold that outlives the
work it guards."""

import asyncio

import pytest

from ptc_agent.agent.claude_code.runtime import TOKEN_CHECK_TIMEOUT_S
from src.server.services.claude_code import checks
from tests.unit.server.services.claude_code.fakes import Cache, serving


@pytest.mark.parametrize(
    ("raw", "answer"),
    [("works", "works"), ("refused", "refused"), ("unknown", "unknown"), (True, "works"), (False, "refused"),
     (None, None), (1, None), (0, None), ("True", None), ({"file": "works"}, None)],
)
def test_a_kept_answer_reads_the_same_whichever_worker_wrote_it(raw, answer):
    assert checks.as_check(raw) == answer


@pytest.mark.asyncio
async def test_an_answer_that_could_not_tell_is_kept_briefly_and_the_others_long():
    cache = Cache()
    with serving(cache):
        for answer in ("works", "refused", "unknown"):
            await checks.keep_check(answer, answer)
    assert cache.ttls == {"works": checks.TOKEN_CHECK_TTL_S, "refused": checks.TOKEN_CHECK_TTL_S, "unknown": checks.SHORT_TTL_S}


@pytest.mark.asyncio
async def test_a_saved_tokens_answer_is_kept_as_workers_from_before_the_words_read_it():
    """A deploy rolls across workers of both kinds, which read each other's answers."""
    cache = Cache()
    with serving(cache):
        for answer in ("works", "refused", "unknown"):
            await checks.keep_check(answer, answer)
            assert await checks.read_check(answer) == answer
    assert cache.values == {"works": True, "refused": False, "unknown": "unknown"}


def test_a_check_is_cut_off_after_the_clis_own_conversation_has_had_its_time():
    assert checks.CHECK_S > TOKEN_CHECK_TIMEOUT_S


@pytest.mark.asyncio
async def test_without_redis_nothing_starts():
    started = []
    with serving(Cache(redis=False)):
        assert await checks.in_background("lock", 10, lambda held: started.append(1)) is None
    assert started == []


@pytest.mark.asyncio
async def test_one_worker_runs_the_work_under_a_hold_that_outlives_it():
    cache, seen = Cache(), []

    async def work(held):
        seen.append((await held(), dict(cache.lock_ttls)))

    with serving(cache):
        assert await checks.in_background("lock", 10, work) is True
        assert await checks.in_background("lock", 10, work) is False, "another worker's is running"
        await asyncio.gather(*checks._checks)
    assert seen == [(True, {"lock": (10 + checks.MARGIN_S) * 1000})] and not cache.locks


@pytest.mark.asyncio
async def test_work_past_its_budget_is_stopped_and_its_hold_released():
    cache, ended = Cache(), []

    async def stuck(held):
        try:
            await asyncio.sleep(60)
        finally:
            ended.append(True)

    with serving(cache):
        await checks.in_background("lock", 0.05, stuck)
        await asyncio.wait_for(asyncio.gather(*checks._checks), 5)
    assert ended == [True] and not cache.locks
