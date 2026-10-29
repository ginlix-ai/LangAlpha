"""watch_market tool behavior (single tool, action="watch"/"unwatch"). Price
alerts are wakeups now; the planning they kept from the old targets is
checked at the bottom against ``src/tools/wakeups/price.py``."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

from src.tools.market_watch.tool import _spellings, watch_market
from src.tools.wakeups.price import already_met, plan_price
from src.tools.wakeups.schedule import PriceAsk, Refused, check_trigger

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
async def test_no_symbols_shows_what_is_watched():
    with patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=["AAPL"])):
        result = await watch_market.ainvoke({}, config=_CFG)
    assert result == "Watching: AAPL."


@pytest.mark.asyncio
async def test_status_says_when_nothing_is_watched():
    with patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=[])):
        result = await watch_market.ainvoke({}, config=_CFG)
    assert result == "Not watching any symbols."


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


@pytest.mark.asyncio
async def test_unwatch_reaches_every_spelling_of_a_symbol():
    with patch(f"{_MOD}.remove_symbols", AsyncMock(return_value=["NVDA"])) as rm:
        result = await watch_market.ainvoke(
            {"symbols": ["^GSPC"], "action": "unwatch"}, config=_CFG
        )
    assert rm.await_args.args == ("t-1", ["^GSPC", "SPX"])
    assert result == "Stopped. Still watching: NVDA."


@pytest.mark.parametrize(
    "symbol, names",
    [
        ("^GSPC", ["^GSPC", "SPX"]),
        ("SPX", ["SPX", "^GSPC"]),
        # The Hang Seng is stored as HSI, which on the watch list is the US
        # ticker, so unwatching the index leaves the stock watched.
        ("^HSI", ["^HSI"]),
        ("HSI", ["HSI"]),
    ],
)
def test_unwatch_reaches_only_the_names_of_one_listing(symbol, names):
    assert _spellings([symbol]) == names


# ---------------------------------------------------------------------------
# Price planning, kept from the old targets
# ---------------------------------------------------------------------------


_NOW = datetime(2026, 10, 6, 15, 0, tzinfo=timezone.utc)


def _refusal(price: dict) -> str:
    try:
        plan_price(check_trigger({"price": price}, _NOW, "UTC"))
    except Refused as e:
        return "\n".join(e.lines)
    raise AssertionError("not refused")


@pytest.mark.parametrize(
    "target, reason",
    [
        ({"symbol": "NVDA", "above": -5}, "greater than 0"),
        ({"symbol": "NVDA", "pct_down": 100}, "never hit"),
        ({"symbol": "BTC-USD", "above": 1}, "crypto, currency or futures"),
        ({"symbol": "ZZZZ.ZZ", "above": 1}, "never be checked"),
        ({"symbol": "!!!", "above": 1}, "not a ticker symbol"),
    ],
)
def test_invalid_price_is_refused_with_its_reason(target, reason):
    assert reason in _refusal(target)


def test_pct_from_is_refused_on_a_level():
    refusal = _refusal({"symbol": "NVDA", "above": 205, "pct_from": "now"})
    assert refusal == "- price: pct_from goes with pct_up or pct_down."


_NY = ZoneInfo("America/New_York")


@pytest.mark.parametrize(
    "now, refused",
    [
        (datetime(2026, 10, 6, 11, 0, tzinfo=_NY), True),  # in session
        # Saturday: the base rolls to Friday's close before the next print,
        # so Friday's move says nothing about the wakeup.
        (datetime(2026, 10, 10, 11, 0, tzinfo=_NY), False),
    ],
)
def test_pct_from_previous_close_is_met_only_while_its_base_is_live(now, refused):
    plan = plan_price(PriceAsk(None, "NVDA", "pct_up", 3.0, "previous_close"))
    reason = already_met(plan, 104.0, 100.0, now)
    assert (reason is not None) is refused
    if refused:
        assert "already up 4.00% from the previous close" in reason
