"""Stream heartbeat: wake the stream's lanes while a model call runs."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from langchain.agents.middleware.types import (
    AgentMiddleware,
    ModelCallResult,
    ModelRequest,
    ModelResponse,
)

from src.utils.stream_coalescing import COALESCE_WINDOW_S, HEARTBEAT_EVENT_TYPE

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable


@asynccontextmanager
async def stream_heartbeat(
    writer: Callable[[Any], None] | None, interval: float = COALESCE_WINDOW_S
) -> AsyncIterator[None]:
    """Write a heartbeat to a graph's custom stream every ``interval`` seconds
    while the block runs."""
    if writer is None:
        yield
        return

    async def beat() -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                writer({"type": HEARTBEAT_EVENT_TYPE})
            except Exception:
                return  # the run's stream is gone; nothing is left to wake

    task = asyncio.create_task(beat())
    try:
        yield
    finally:
        task.cancel()


def _stream_writer() -> Callable[[Any], None] | None:
    try:
        from langgraph.config import get_stream_writer

        return get_stream_writer()
    except Exception:
        # Outside a streaming graph run (tests, sync invocations).
        return None


class StreamHeartbeatMiddleware(AgentMiddleware):
    """Beat on the graph's custom stream while a model call runs.

    The delta coalescer has no timer: a lane checks its held delta's window
    only when a graph event wakes it, and a model can go quiet mid-message
    (between reasoning-summary parts, between thinking bursts) with its last
    words held. The beat wakes the lane so they go out on time. It also drives
    the run stream's timeout check, which runs once per graph event. Outer to
    compaction, so a summary call beats as well as the model call.
    """

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelCallResult:
        # Sync fallback: the async agent won't call this but the protocol requires it.
        return handler(request)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelCallResult:
        async with stream_heartbeat(_stream_writer()):
            return await handler(request)
