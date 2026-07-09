"""watch_market / unwatch_market tool behavior."""

from unittest.mock import AsyncMock, patch

import pytest

from src.tools.market_watch.tool import unwatch_market, watch_market

_MOD = "src.tools.market_watch.tool"
_CFG = {"configurable": {"thread_id": "t-1"}}


# RunnableConfig is an injected tool arg: it is passed as the second positional
# arg to ainvoke (config=...), NOT as a key inside the tool-input dict.
@pytest.mark.asyncio
async def test_watch_registers_symbols():
    with patch(f"{_MOD}.add_symbols", AsyncMock(return_value=["NVDA", "TSLA"])) as mock_add:
        result = await watch_market.ainvoke({"symbols": ["NVDA", "TSLA"]}, config=_CFG)
    mock_add.assert_awaited_once_with("t-1", ["NVDA", "TSLA"])
    assert "NVDA, TSLA" in result
    assert "<market-watch>" in result


@pytest.mark.asyncio
async def test_watch_without_thread_id_errors_gracefully():
    result = await watch_market.ainvoke({"symbols": ["NVDA"]}, config={"configurable": {}})
    assert "unavailable" in result.lower()


@pytest.mark.asyncio
async def test_unwatch_specific():
    with patch(f"{_MOD}.remove_symbols", AsyncMock(return_value=["TSLA"])):
        result = await unwatch_market.ainvoke({"symbols": ["NVDA"]}, config=_CFG)
    assert "TSLA" in result


@pytest.mark.asyncio
async def test_unwatch_all():
    with patch(f"{_MOD}.remove_symbols", AsyncMock(return_value=[])) as mock_rm:
        result = await unwatch_market.ainvoke({}, config=_CFG)
    mock_rm.assert_awaited_once_with("t-1", None)
    assert "stopped" in result.lower()
