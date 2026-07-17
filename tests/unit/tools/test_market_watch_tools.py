"""watch_market tool behavior (single tool, action="watch"/"unwatch")."""

from unittest.mock import AsyncMock, patch

import pytest

from src.tools.market_watch.tool import watch_market

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
async def test_watch_result_instructs_feed_usage():
    """The watch result carries the action-specific unwatch instruction.

    Feed mechanics live in the market-watch skill; the result only needs the
    reciprocal action (how to stop) plus what the feed looks like.
    """
    with patch(f"{_MOD}.add_symbols", AsyncMock(return_value=["NVDA"])):
        result = await watch_market.ainvoke({"symbols": ["NVDA"]}, config=_CFG)
    assert "<market-watch>" in result
    assert 'action="unwatch"' in result


@pytest.mark.asyncio
async def test_watch_without_thread_id_errors_gracefully():
    result = await watch_market.ainvoke({"symbols": ["NVDA"]}, config={"configurable": {}})
    assert "unavailable" in result.lower()


@pytest.mark.asyncio
async def test_watch_without_symbols_asks_for_symbols():
    result = await watch_market.ainvoke({}, config=_CFG)
    assert "symbols" in result.lower()


@pytest.mark.asyncio
async def test_unwatch_specific():
    with patch(f"{_MOD}.remove_symbols", AsyncMock(return_value=["TSLA"])):
        result = await watch_market.ainvoke(
            {"symbols": ["NVDA"], "action": "unwatch"}, config=_CFG
        )
    assert "TSLA" in result


@pytest.mark.asyncio
async def test_unwatch_all():
    with patch(f"{_MOD}.remove_symbols", AsyncMock(return_value=[])) as mock_rm:
        result = await watch_market.ainvoke({"action": "unwatch"}, config=_CFG)
    mock_rm.assert_awaited_once_with("t-1", None)
    assert "stopped" in result.lower()


@pytest.mark.asyncio
async def test_watch_cache_unavailable():
    with patch(f"{_MOD}.add_symbols", AsyncMock(return_value=None)):
        result = await watch_market.ainvoke({"symbols": ["NVDA"]}, config=_CFG)
    assert "unavailable" in result.lower()


@pytest.mark.asyncio
async def test_watch_all_symbols_invalid():
    with patch(f"{_MOD}.add_symbols", AsyncMock(return_value=[])):
        result = await watch_market.ainvoke({"symbols": ["!!!"]}, config=_CFG)
    assert "no valid" in result.lower()


@pytest.mark.asyncio
async def test_unwatch_cache_unavailable_is_not_reported_as_stopped():
    """A failed mutation must never read as success — the Redis key survives
    and injection would resume once the cache recovers."""
    with patch(f"{_MOD}.remove_symbols", AsyncMock(return_value=None)):
        result = await watch_market.ainvoke({"action": "unwatch"}, config=_CFG)
    assert "unavailable" in result.lower()
    assert "stopped" not in result.lower()


@pytest.mark.asyncio
async def test_watch_reports_rejected_symbols():
    """A dropped symbol (bad grammar or cap overflow) must not read as success:
    with a non-empty watchlist, "Now watching: AAPL." alone is indistinguishable
    from the requested symbol having been added."""
    with patch(f"{_MOD}.add_symbols", AsyncMock(return_value=["AAPL"])):
        result = await watch_market.ainvoke({"symbols": ["SPX 500"]}, config=_CFG)
    assert "Now watching: AAPL" in result
    assert "Not added" in result
    assert "SPX 500" in result


@pytest.mark.asyncio
async def test_watch_no_rejection_note_when_all_added():
    # Lowercase input normalizes to the accepted symbol — not a rejection.
    with patch(f"{_MOD}.add_symbols", AsyncMock(return_value=["AAPL", "NVDA"])):
        result = await watch_market.ainvoke({"symbols": ["nvda"]}, config=_CFG)
    assert "Not added" not in result
