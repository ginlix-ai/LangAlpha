"""A shared thread's stream ends once its viewer loses access: no channel
opens for a run that starts after, and the socket ends as a timed out one."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.server.handlers.chat import thread_stream_mux_v2 as mux

THREAD = "t-1"
RUN = "r-new"


@pytest.fixture
def stream(monkeypatch):
    """The mux over a fake transport whose control stream announces one new
    run on the first read and is quiet after."""
    announced = {"done": False}

    async def xread(streams, block=None, count=None):
        if not announced["done"]:
            announced["done"] = True
            control = mux.control_stream_key(THREAD).encode()
            return [(control, [(b"1-0", {b"type": b"run_started", b"run_id": RUN.encode()})])]
        await asyncio.sleep(0.01)
        return None

    cache = MagicMock()
    cache.enabled = True
    cache.client.xrevrange = AsyncMock(return_value=[])
    cache.client.xrange = AsyncMock(return_value=[])
    reader = MagicMock()
    reader.xread = xread

    async def quiet_watch(*_a, **_k):
        await asyncio.sleep(3600)
        yield ""

    async def nothing(*_a, **_k):
        return []

    monkeypatch.setattr(mux, "get_cache_client", lambda: cache)
    monkeypatch.setattr(mux.stream_pool, "get_stream_reader_client", AsyncMock(return_value=reader))
    monkeypatch.setattr(mux, "watch_wakes", lambda *_a: quiet_watch())
    monkeypatch.setattr(mux, "_seed_channels", nothing)
    monkeypatch.setattr(mux, "_load_pending", AsyncMock(return_value={}))
    monkeypatch.setattr(mux, "_admission_meta", AsyncMock(return_value=(False, 1.0)))
    monkeypatch.setattr(mux, "_probe_backlog_head", AsyncMock(return_value=None))
    monkeypatch.setattr(mux, "_KEEPALIVE_S", 0.05)

    async def frames(recheck, until):
        out = []
        gen = mux.stream_thread_mux_v2(THREAD, {}, watch=SimpleNamespace(ok=lambda force=False: recheck()))
        try:
            async with asyncio.timeout(2):
                async for frame in gen:
                    out.append(frame)
                    if until(frame):
                        break
        finally:
            await gen.aclose()
        return out

    return frames


@pytest.mark.asyncio
async def test_a_viewer_who_lost_access_gets_no_new_run(stream):
    asked = []

    async def recheck():
        asked.append(1)
        return False

    out = await stream(recheck, lambda f: "event: timeout" in f or "chan_open" in f)

    assert not any("chan_open" in f for f in out)
    assert "event: timeout" in out[-1]
    assert asked


@pytest.mark.asyncio
async def test_a_viewer_who_still_has_access_gets_it(stream):
    out = await stream(AsyncMock(return_value=True), lambda f: "chan_open" in f)

    assert RUN in out[-1]


@pytest.mark.asyncio
async def test_a_frame_goes_out_only_while_the_viewer_still_reaches_the_thread(stream, monkeypatch):
    """The full check let the run in; the thread then went private, which the
    check before each frame sees, so its first frame never goes out."""
    watch = SimpleNamespace(ok=AsyncMock(side_effect=lambda force=False: force))
    out = []
    gen = mux.stream_thread_mux_v2(THREAD, {}, watch=watch)
    try:
        async with asyncio.timeout(2):
            async for frame in gen:
                out.append(frame)
                if "event: timeout" in frame:
                    break
    finally:
        await gen.aclose()

    assert not any("chan_open" in f for f in out)
    assert any(c.kwargs == {"force": True} for c in watch.ok.call_args_list)
