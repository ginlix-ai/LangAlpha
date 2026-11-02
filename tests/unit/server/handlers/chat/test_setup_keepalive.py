"""What a turn's response sends while its setup is silent, on the real wrapper
a deployment serves it through."""

from __future__ import annotations

import asyncio

import pytest
from starlette.middleware.gzip import GZipMiddleware

from src.server.handlers.chat.setup_keepalive import SetupKeepaliveResponse

KEEPALIVE = b":keepalive\n\n"
FRAME = b"event: metadata\ndata: {}\n\n"


class Wire:
    """An ASGI server's side of one response, with disconnect under its control."""

    def __init__(self):
        self.messages: list[dict] = []
        self.heard = asyncio.Event()
        self.disconnect = asyncio.Event()

    async def send(self, message):
        self.messages.append(message)
        self.heard.set()

    async def receive(self):
        await self.disconnect.wait()
        return {"type": "http.disconnect"}

    @property
    def bodies(self) -> list[bytes]:
        return [
            m["body"] for m in self.messages if m["type"] == "http.response.body"
        ]

    async def wait_for_body(self, body: bytes, count: int = 1):
        async def until():
            while self.bodies.count(body) < count:
                self.heard.clear()
                await self.heard.wait()

        await asyncio.wait_for(until(), 2)


def _serve(wire, stream, *, interval, setup_over, app_wrapper=None):
    response = SetupKeepaliveResponse(
        stream,
        media_type="text/event-stream",
        interval=interval,
        setup_over=setup_over,
    )
    app = app_wrapper(response) if app_wrapper else response
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "headers": [(b"accept-encoding", b"gzip")],
        # uvicorn's level, where the response listens for the disconnect itself.
        "asgi": {"spec_version": "2.3"},
    }
    return asyncio.create_task(app(scope, wire.receive, wire.send))


@pytest.mark.asyncio
async def test_the_first_beat_goes_out_before_the_setup_yields_anything():
    # GZipMiddleware holds the response's start until its first body, so without
    # one the client's fetch stays pending, headers included, through the setup.
    wire = Wire()
    release = asyncio.Event()

    async def stream():
        await release.wait()
        yield FRAME

    served = _serve(
        wire,
        stream(),
        interval=60,
        setup_over=asyncio.Event(),
        app_wrapper=lambda r: GZipMiddleware(r, minimum_size=0),
    )
    await wire.wait_for_body(KEEPALIVE)

    assert wire.messages[0]["type"] == "http.response.start"

    release.set()
    await asyncio.wait_for(served, 2)
    assert wire.bodies[-1] == b""


@pytest.mark.asyncio
async def test_it_beats_until_the_stream_takes_over():
    wire = Wire()
    setup_over = asyncio.Event()
    release = asyncio.Event()

    async def stream():
        await release.wait()
        setup_over.set()
        yield FRAME
        await asyncio.sleep(0.05)
        yield FRAME

    served = _serve(wire, stream(), interval=0.01, setup_over=setup_over)
    await wire.wait_for_body(KEEPALIVE, count=3)

    release.set()
    await asyncio.wait_for(served, 2)

    after_first_frame = wire.bodies[wire.bodies.index(FRAME) :]
    assert KEEPALIVE not in after_first_frame
    assert after_first_frame == [FRAME, FRAME, b""]


@pytest.mark.asyncio
async def test_nothing_goes_out_after_the_last_message():
    # A setup that fails yields its frame and ends without the stream taking
    # over, so the beat is still running when the response closes.
    wire = Wire()

    async def stream():
        await wire.wait_for_body(KEEPALIVE)
        yield FRAME

    served = _serve(wire, stream(), interval=0.001, setup_over=asyncio.Event())
    await asyncio.wait_for(served, 2)
    await asyncio.sleep(0.01)

    assert wire.bodies[-1] == b""
    assert wire.messages[-1].get("more_body", False) is False


@pytest.mark.asyncio
async def test_a_disconnect_still_lands_in_the_setup():
    # The run's settlement hangs off the generator seeing its cancel.
    wire = Wire()
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def stream():
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        yield FRAME

    served = _serve(wire, stream(), interval=60, setup_over=asyncio.Event())
    await asyncio.wait_for(entered.wait(), 2)

    wire.disconnect.set()

    await asyncio.wait_for(cancelled.wait(), 2)
    await asyncio.wait_for(served, 2)
