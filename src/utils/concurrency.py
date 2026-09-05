"""Shared asyncio helpers for background-task lifecycle."""

import asyncio


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
