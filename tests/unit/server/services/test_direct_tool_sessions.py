"""Opening a turn's relay sessions: together, and none of them unbounded."""

import asyncio
import time

import pytest

from src.server.services.egress import direct_tools
from src.server.services.egress.direct_tools import DirectMCPBinding


class _Stalled:
    async def __aenter__(self):
        await asyncio.sleep(3600)

    async def __aexit__(self, *a):
        return False


class _Fast:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


async def _one_event():
    yield "first-token"


@pytest.mark.asyncio
async def test_a_stalled_session_does_not_hold_the_first_token(monkeypatch):
    """Opening early buys one handshake; it must not cost the whole turn."""
    monkeypatch.setattr(direct_tools, "SESSION_OPEN_TIMEOUT_S", 0.05)
    binding = DirectMCPBinding(user_id="u")
    binding.add_server("stalled", _Stalled())
    binding.add_server("fine", _Fast())

    started = time.perf_counter()
    events = [e async for e in binding.drive(_one_event())]
    elapsed = time.perf_counter() - started

    assert events == ["first-token"]
    # The content alone would still pass if the bound regressed to a minute,
    # and being late is the whole defect here.
    assert elapsed < 0.5


@pytest.mark.asyncio
async def test_sessions_open_together_rather_than_in_turn(monkeypatch):
    monkeypatch.setattr(direct_tools, "SESSION_OPEN_TIMEOUT_S", 5.0)

    class _Slow:
        async def __aenter__(self):
            await asyncio.sleep(0.2)
            return self

        async def __aexit__(self, *a):
            return False

    binding = DirectMCPBinding(user_id="u")
    for i in range(4):
        binding.add_server(f"s{i}", _Slow())

    started = time.perf_counter()
    events = [e async for e in binding.drive(_one_event())]
    elapsed = time.perf_counter() - started

    assert events == ["first-token"]
    # Four 0.2s opens: together they cost one of them, in turn they cost four.
    assert elapsed < 0.6
