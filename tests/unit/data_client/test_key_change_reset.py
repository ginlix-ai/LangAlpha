"""A changed vendor key reaches market data without cutting off what runs: a
chain built under the old key is never kept, and the old client closes only
once its requests finish."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.data_client import fmp, registry
from src.data_client.fmp import FMPClient


@pytest.mark.asyncio
async def test_a_chain_built_while_the_key_changes_is_not_kept(monkeypatch):
    gate = asyncio.Event()

    async def build():
        await gate.wait()
        return MagicMock()

    monkeypatch.setitem(registry._SOURCE_REGISTRY, "slow", (lambda: True, build))
    monkeypatch.setattr(
        "src.config.settings.get_market_data_providers", lambda: [{"name": "slow"}]
    )
    monkeypatch.setattr(registry, "_load_routing_table", lambda: None)
    monkeypatch.setattr(registry, "_market_data_provider", None)

    building = asyncio.create_task(registry.get_market_data_provider())
    await asyncio.sleep(0)
    reset = asyncio.create_task(registry.reset_data_providers())
    await asyncio.sleep(0)
    assert not reset.done()

    gate.set()
    await building
    await reset
    assert registry._market_data_provider is None


@pytest.mark.asyncio
async def test_the_old_client_closes_once_its_requests_finish(monkeypatch):
    gate = asyncio.Event()

    async def request(*_args):
        await gate.wait()
        return {"ok": True}

    old = FMPClient(api_key="old")
    monkeypatch.setattr(old, "_request", request)
    monkeypatch.setattr(old, "close", AsyncMock())
    monkeypatch.setattr(fmp, "_fmp_client", old)

    running = asyncio.create_task(old._make_request("profile", use_cache=False))
    await asyncio.sleep(0)
    await fmp.replace_fmp_client()
    old.close.assert_not_awaited()

    gate.set()
    assert await running == {"ok": True}
    old.close.assert_awaited_once()

    monkeypatch.setenv("FMP_API_KEY", "new")
    fresh = await fmp.get_fmp_client()
    assert fresh is not old
    assert fresh.api_key == "new"
    monkeypatch.setattr(fmp, "_fmp_client", None)
