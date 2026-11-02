"""Keeps a send's response audible until its run's stream takes over.

Setup (an admission hold, a computer starting) can last minutes and sends
nothing of its own, so a client cannot tell a slow setup from a dead socket,
and an edge proxy may cut what looks idle. The beat comes from beside the turn
generator, not through it: a disconnect has to keep landing in the generator's
own awaits, where it cancels the setup, and running the setup on a task of its
own to race a timer would move that. The generator only says when its stream
has taken over, whose reader keeps it alive from then on.
"""

import asyncio
import contextlib
import logging

from starlette.responses import StreamingResponse
from starlette.types import Message, Send

logger = logging.getLogger(__name__)

_KEEPALIVE: Message = {
    "type": "http.response.body",
    "body": b":keepalive\n\n",
    "more_body": True,
}


class SetupKeepaliveResponse(StreamingResponse):
    """A turn's response, with a keepalive each ``interval`` seconds until
    ``setup_over`` is set."""

    def __init__(self, *args, interval: float, setup_over: asyncio.Event, **kwargs):
        super().__init__(*args, **kwargs)
        self._interval = interval
        self._setup_over = setup_over

    async def stream_response(self, send: Send) -> None:
        # The beat's writes interleave with the generator's frames, and an ASGI
        # send takes one message at a time, none past the response's last.
        lock = asyncio.Lock()
        closed = False

        async def one_at_a_time(message: Message) -> None:
            nonlocal closed
            async with lock:
                if closed:
                    return
                closed = message["type"] == "http.response.body" and not message.get(
                    "more_body", False
                )
                await send(message)

        # First at once: GZipMiddleware holds the response's start back until
        # its first body, so a client hears nothing, headers included, until
        # then. The task runs after this one has taken the lock for the start.
        async def beat() -> None:
            while True:
                await one_at_a_time(_KEEPALIVE)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._setup_over.wait(), self._interval)
                    return

        beating = asyncio.create_task(beat(), name="setup-keepalive")
        beating.add_done_callback(_log_failure)
        try:
            await super().stream_response(one_at_a_time)
        finally:
            beating.cancel()


def _log_failure(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.debug("[PTC_CHAT] Setup keepalive stopped", exc_info=task.exception())
