"""Shared asyncio helpers for background-task lifecycle."""

import asyncio
import contextvars
import logging
import time
from collections.abc import AsyncIterator, Callable
from typing import Any

logger = logging.getLogger(__name__)

# Closes in flight, held only so the loop cannot collect one left running past
# its bound; nothing reads it.
_closing: set[asyncio.Task] = set()
_END = object()


async def cancel_and_join(task: asyncio.Task) -> None:
    """Cancel a background task and await it, absorbing only ITS failure.

    The obvious ``except (asyncio.CancelledError, Exception)`` also swallows a
    ``CancelledError`` aimed at the *caller* — a shutdown step hitting its own
    deadline — silently turning "abort the shutdown" into "carry on". The
    discriminator has to be ``current_task().cancelling()``, not
    ``task.cancelled()``: cancelling a caller that is blocked on ``await task``
    propagates *into* that task, so the inner task ends cancelled either way
    and only the caller's own pending-cancel count tells the two apart.
    """
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        caller = asyncio.current_task()
        if caller is not None and caller.cancelling() > 0:
            raise
    except Exception:
        pass


async def aclose_within(
    stream: Any,
    timeout: float,
    *,
    label: str,
    track: Callable[[asyncio.Task], None] | None = None,
) -> None:
    """Close an async iterator, waiting at most ``timeout`` for its cleanup.

    The close runs as its own task so a cleanup that will not finish (LangGraph
    awaits its cancelled node tasks with no bound, and a tool's shielded cleanup
    can retry for minutes) is left running past the bound instead of holding the
    caller. A cancel of the caller leaves it running too. ``track`` receives
    the close task, so a caller can count it as running until it ends, past
    the bound included. Failures are logged, never raised: the caller is
    already on its way out.
    """
    aclose = getattr(stream, "aclose", None)
    if aclose is None:
        return

    async def _close() -> None:
        await aclose()

    await _await_within(
        asyncio.create_task(_close(), name=f"aclose-{label}"),
        timeout,
        label=label,
        track=track,
    )


async def _await_within(
    task: asyncio.Task,
    timeout: float,
    *,
    label: str,
    track: Callable[[asyncio.Task], None] | None,
) -> None:
    _closing.add(task)

    def _reap(t: asyncio.Task) -> None:
        _closing.discard(t)
        if not t.cancelled() and t.exception() is not None:
            logger.warning(f"closing {label} failed", exc_info=t.exception())

    task.add_done_callback(_reap)
    if track is not None:
        track(task)
    done, _ = await asyncio.wait({task}, timeout=timeout)
    if not done:
        logger.warning(
            f"closing {label} is still running after {timeout:g}s; "
            f"leaving it to finish on its own"
        )


async def iterate_until(
    stream: Any,
    stop_at: float | None,
    *,
    on_deadline: Callable[[], BaseException],
    close_timeout: float,
    label: str,
    track: Callable[[asyncio.Task], None] | None = None,
) -> AsyncIterator[Any]:
    """Iterate ``stream`` until the wall-clock time ``stop_at``, then raise
    ``on_deadline()``.

    Each wait for the next item is its own task, on one context kept across
    items as ``async for`` would keep it, so the deadline ends a wait inside an
    item that never comes and no cancel lands in the caller's own code. Leaving
    early would leave the stream suspended mid-step, its work running on until
    the garbage collector closes it, so it is closed within ``close_timeout``;
    a wait cut short by the deadline is cancelled within the same bound, since
    a generator cannot be closed mid-step. A stop from outside instead cancels
    the wait in flight and joins it without a bound, as ``async for`` did, so
    the stream has stopped working before the stop goes on. A further stop
    neither cuts that join short nor reaches the stream, whose own exit may be
    waiting on work it already started. ``track`` receives each close and
    cut-short step it waits on within ``close_timeout``, as ``aclose_within``
    does.
    """
    items = aiter(stream)
    context = contextvars.copy_context()
    step: asyncio.Task | None = None

    async def next_item() -> Any:
        return await anext(items, _END)

    try:
        while True:
            remaining = None if stop_at is None else stop_at - time.time()
            if remaining is not None and remaining <= 0:
                raise on_deadline()
            step = asyncio.create_task(next_item(), context=context)
            try:
                await asyncio.wait({step}, timeout=remaining)
            except asyncio.CancelledError:
                step.cancel()
                while not step.done():
                    try:
                        await asyncio.wait({step})
                    except asyncio.CancelledError:
                        pass
                raise
            if not step.done():
                step.cancel()
                await _await_within(
                    step, close_timeout, label=label, track=track
                )
                raise on_deadline()
            item = step.result()
            if item is _END:
                return
            yield item
    finally:
        if step is None or step.done():
            await aclose_within(
                stream, close_timeout, label=label, track=track
            )
