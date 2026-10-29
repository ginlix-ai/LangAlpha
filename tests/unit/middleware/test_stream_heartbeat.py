"""StreamHeartbeatMiddleware: beats on the custom stream only while a model call runs."""

from __future__ import annotations

import asyncio

import pytest

from ptc_agent.agent.middleware.stream_heartbeat import StreamHeartbeatMiddleware
from src.utils.stream_coalescing import HEARTBEAT_EVENT_TYPE


@pytest.fixture
def beats(monkeypatch):
    captured: list[dict] = []
    monkeypatch.setattr("langgraph.config.get_stream_writer", lambda: captured.append)
    return captured


async def _until_a_beat(beats: list[dict]) -> None:
    while not beats:
        await asyncio.sleep(0.01)


async def _assert_quiet_after(beats: list[dict]) -> None:
    seen = len(beats)
    await asyncio.sleep(0.25)
    assert len(beats) == seen


@pytest.mark.asyncio
async def test_a_call_beats_until_it_returns(beats):
    async def handler(request):
        await _until_a_beat(beats)
        return "ok"

    call = StreamHeartbeatMiddleware().awrap_model_call(object(), handler)
    assert await asyncio.wait_for(call, timeout=5) == "ok"
    assert beats[0] == {"type": HEARTBEAT_EVENT_TYPE}
    await _assert_quiet_after(beats)


@pytest.mark.asyncio
async def test_a_call_that_raises_stops_the_beat(beats):
    async def handler(request):
        await _until_a_beat(beats)
        raise RuntimeError("model failed")

    call = StreamHeartbeatMiddleware().awrap_model_call(object(), handler)
    with pytest.raises(RuntimeError, match="model failed"):
        await asyncio.wait_for(call, timeout=5)
    await _assert_quiet_after(beats)


@pytest.mark.asyncio
async def test_outside_a_graph_run_there_is_no_beat():
    before = asyncio.all_tasks()

    async def handler(request):
        await asyncio.sleep(0.25)
        assert asyncio.all_tasks() == before
        return "ok"

    assert await StreamHeartbeatMiddleware().awrap_model_call(object(), handler) == "ok"
