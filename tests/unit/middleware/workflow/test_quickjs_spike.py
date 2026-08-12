"""Spike tests pinning the quickjs_rs behaviors the workflow engine depends on.

Each test locks one load-bearing assumption from the v2 engine design:
promise-parked host calls outliving the eval timeout, CPU-spin interruption,
GIL release during eval, caller-side cancellation unwinding the host call,
and the async-IIFE + TOP_LEVEL_CONST_TO_VAR source transform round-trip.
"""

import asyncio
import time

import pytest
import quickjs_rs
from quickjs_rs import Runtime, SourceTransform, ThreadWorker


def _make_context(worker: ThreadWorker, *, timeout: float = 5.0):
    async def _build():
        runtime = Runtime(
            memory_limit=128 * 1024 * 1024,
            transform_flags=SourceTransform.TOP_LEVEL_CONST_TO_VAR,
        )
        ctx = runtime.new_context(timeout=timeout)
        return runtime, ctx

    return worker.run_sync(_build())


@pytest.fixture
def worker():
    w = ThreadWorker(name="quickjs-spike")
    yield w
    w.close()


@pytest.mark.asyncio
async def test_promise_parking_outlives_eval_timeout(worker):
    """A host call sleeping past the eval timeout must not trip the deadline.

    The engine design sets ``timeout=cpu_budget_s`` to bound continuous JS,
    while subagent dispatches park the script for minutes. If the wall-clock
    deadline fired across host awaits the design falls back to timeout=None.
    """
    runtime, ctx = _make_context(worker, timeout=1.0)

    async def _run():
        async def slow_host():
            await asyncio.sleep(3.0)
            return "done"

        ctx.register("slowHost", slow_host, is_async=True)
        handle = await ctx.eval_handle_async(
            "(async () => { const r = await slowHost(); return r + '!'; })()",
            timeout=1.0,
        )
        with handle:
            resolved = await handle.await_promise(timeout=10.0)
            with resolved:
                return resolved.to_python()

    result = await asyncio.wrap_future(worker.run_async(_run()))
    assert result == "done!"


@pytest.mark.asyncio
async def test_cpu_spin_is_interrupted_near_budget(worker):
    """``while(true)`` must be killed by the interrupt handler near the budget."""
    runtime, ctx = _make_context(worker, timeout=1.0)

    async def _run():
        handle = await ctx.eval_handle_async("while (true) {}", timeout=1.0)
        handle.dispose()

    start = time.monotonic()
    with pytest.raises((quickjs_rs.TimeoutError, quickjs_rs.InterruptError)):
        await asyncio.wrap_future(worker.run_async(_run()))
    elapsed = time.monotonic() - start
    assert elapsed < 5.0, f"interrupt took {elapsed:.1f}s, expected ~1s"


@pytest.mark.asyncio
async def test_gil_released_during_cpu_spin(worker):
    """The event loop must keep making progress while JS spins on the worker."""
    runtime, ctx = _make_context(worker, timeout=1.0)

    async def _run():
        handle = await ctx.eval_handle_async("while (true) {}", timeout=1.0)
        handle.dispose()

    spin = asyncio.wrap_future(worker.run_async(_run()))
    ticks = 0
    while not spin.done():
        await asyncio.sleep(0.01)
        ticks += 1
    with pytest.raises((quickjs_rs.TimeoutError, quickjs_rs.InterruptError)):
        spin.result()
    assert ticks > 10, f"only {ticks} loop ticks during ~1s spin — GIL likely held"


@pytest.mark.asyncio
async def test_cancellation_unwinds_parked_host_call(worker):
    """Cancelling the caller-side future must unwind a parked host call."""
    runtime, ctx = _make_context(worker, timeout=5.0)
    host_cancelled = asyncio.Event()
    outer_loop = asyncio.get_running_loop()

    async def _run():
        async def park_forever():
            try:
                await asyncio.sleep(60.0)
            except asyncio.CancelledError:
                outer_loop.call_soon_threadsafe(host_cancelled.set)
                raise
            return "never"

        ctx.register("parkForever", park_forever, is_async=True)
        handle = await ctx.eval_handle_async(
            "(async () => await parkForever())()", timeout=5.0
        )
        with handle:
            resolved = await handle.await_promise(timeout=None)
            with resolved:
                return resolved.to_python()

    fut = worker.run_async(_run())
    wrapped = asyncio.wrap_future(fut)
    await asyncio.sleep(0.5)
    wrapped.cancel()
    with pytest.raises(asyncio.CancelledError):
        await wrapped
    async with asyncio.timeout(5.0):
        await host_cancelled.wait()


@pytest.mark.asyncio
async def test_async_iife_wrap_and_const_transform_roundtrip(worker):
    """The engine's source shape: strip ``export``, eval meta literal, wrap body."""
    runtime, ctx = _make_context(worker, timeout=5.0)

    script = (
        "export const meta = { name: 'demo', description: 'spike' }\n"
        "const items = args.items\n"
        "const doubled = items.map(x => x * 2)\n"
        "return { doubled, total: doubled.reduce((a, b) => a + b, 0) }\n"
    )

    async def _run():
        body = script.replace("export const meta", "const meta", 1)
        wrapped = f"(async () => {{\n{body}\n}})()"
        ctx.globals["args"] = {"items": [1, 2, 3]}
        handle = await ctx.eval_handle_async(wrapped, timeout=5.0)
        with handle:
            resolved = await handle.await_promise(timeout=5.0)
            with resolved:
                run_result = resolved.to_python()
        # TOP_LEVEL_CONST_TO_VAR: a top-level const in a *separate* eval must
        # tolerate re-declaration (meta extraction happens in its own eval).
        meta1 = await ctx.eval_async("const meta = { name: 'demo' }; meta")
        meta2 = await ctx.eval_async("const meta = { name: 'demo2' }; meta")
        return run_result, meta1, meta2

    run_result, meta1, meta2 = await asyncio.wrap_future(worker.run_async(_run()))
    assert run_result == {"doubled": [2, 4, 6], "total": 12}
    assert meta1 == {"name": "demo"}
    assert meta2 == {"name": "demo2"}
