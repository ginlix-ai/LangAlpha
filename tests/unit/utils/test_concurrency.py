import asyncio

import pytest

from src.utils.concurrency import cancel_and_join


@pytest.mark.asyncio
async def test_absorbs_the_cancellation_it_issues():
    async def forever():
        await asyncio.Event().wait()

    task = asyncio.create_task(forever())
    await asyncio.sleep(0)

    await cancel_and_join(task)  # must not raise
    assert task.cancelled()


@pytest.mark.asyncio
async def test_absorbs_a_task_that_raised():
    async def boom():
        raise RuntimeError("background failure")

    task = asyncio.create_task(boom())
    await asyncio.sleep(0)

    await cancel_and_join(task)  # a dying background task never fails shutdown


@pytest.mark.asyncio
async def test_a_task_that_ignores_cancellation_is_joined_not_swallowed():
    # Suppressing its own CancelledError means the task ends normally, so
    # task.cancelled() is False — and there is nothing to re-raise either.
    async def stubborn():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return "finished anyway"

    task = asyncio.create_task(stubborn())
    await asyncio.sleep(0)

    await cancel_and_join(task)
    assert task.cancelled() is False
    assert task.result() == "finished anyway"


@pytest.mark.asyncio
async def test_cancellation_aimed_at_the_caller_keeps_propagating():
    # The regression this helper exists for: a shutdown step hitting its own
    # deadline must abort, not be silently absorbed into "carry on".
    #
    # Note the inner task ends CANCELLED here — cancelling a caller blocked on
    # `await task` propagates into that task — which is precisely why
    # `task.cancelled()` cannot be the discriminator.
    started = asyncio.Event()

    async def slow_to_die():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(10)  # outlives the caller's deadline

    async def caller():
        task = asyncio.create_task(slow_to_die())
        await asyncio.sleep(0)
        started.set()
        await cancel_and_join(task)

    outer = asyncio.create_task(caller())
    await started.wait()
    await asyncio.sleep(0)

    outer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await outer
