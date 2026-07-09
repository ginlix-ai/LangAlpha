"""MarketWatchMiddleware injection behavior (single-carrier abefore_model)."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytz
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.market_watch import MarketWatchMiddleware
from src.market_protocol import MarketPhase

_MOD = "ptc_agent.agent.middleware.market_watch"
_ET = pytz.timezone("US/Eastern")
_FIXED_ET = _ET.localize(datetime(2026, 7, 1, 14, 32, 5))
_CFG = {"configurable": {"thread_id": "t-1", "user_id": "u-1"}}

_SNAPS = [{"symbol": "NVDA", "price": 231.0, "change_percent": 2.31,
           "volume": 1_000_000, "last_trade_price": 233.45, "market_status": "open"}]


def _fake_calendar(phase):
    """A calendar whose phase_at returns a fixed MarketPhase for the venue gate."""
    cal = MagicMock()
    cal.phase_at = MagicMock(return_value=phase)
    return cal


def _mw(interval=25):
    return MarketWatchMiddleware(min_interval_seconds=interval)


def _runtime():
    rt = MagicMock()
    rt.stream_writer = MagicMock()
    return rt


def _patched(watchlist, snaps=_SNAPS, phase=MarketPhase.REGULAR):
    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(return_value=snaps)
    return (
        patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=watchlist)),
        patch(f"{_MOD}.get_market_data_provider", return_value=provider),
        patch("src.tools.market_data.quote_format.get_market_session",
              return_value=("REGULAR_HOURS", _FIXED_ET)),
        patch(f"{_MOD}.get_calendar", return_value=_fake_calendar(phase)),
    )


def _human_state():
    """Turn state whose tail is the incoming human message (the stamp target)."""
    return {"messages": [HumanMessage(content="What is NVDA doing?", id="h-1")]}


def _batch_state(tool_msgs, tool_calls=None):
    """Turn state whose tail is a just-completed tool batch (Human → AI → tools)."""
    ai = AIMessage(content="", tool_calls=tool_calls or [], id="ai-1")
    return {"messages": [HumanMessage(content="hi", id="h-0"), ai, *tool_msgs]}


# --- human message stamp ----------------------------------------------------


@pytest.mark.asyncio
async def test_stamps_human_str_content():
    mw = _mw()
    p1, p2, p3, p4 = _patched(["NVDA"])
    with p1, p2, p3, p4:
        update = await mw.abefore_model(_human_state(), _runtime(), config=_CFG)
    msgs = update["messages"]
    assert len(msgs) == 1
    out = msgs[0]
    assert isinstance(out, HumanMessage)
    assert out.id == "h-1"  # same id → add_messages replaces in place
    assert "What is NVDA doing?" in out.content
    assert "<market-watch>" in out.content
    assert "</market-watch>" in out.content
    assert "$233.45" in out.content


@pytest.mark.asyncio
async def test_stamps_human_list_content():
    mw = _mw()
    parts = [{"type": "text", "text": "What is NVDA doing?"}]
    state = {"messages": [HumanMessage(content=parts, id="h-2")]}
    p1, p2, p3, p4 = _patched(["NVDA"])
    with p1, p2, p3, p4:
        update = await mw.abefore_model(state, _runtime(), config=_CFG)
    out = update["messages"][0]
    assert out.id == "h-2"
    assert isinstance(out.content, list)
    # original part untouched, block appended as an extra text part
    assert out.content[0] == {"type": "text", "text": "What is NVDA doing?"}
    assert len(out.content) == 2
    assert out.content[-1]["type"] == "text"
    assert "<market-watch>" in out.content[-1]["text"]
    assert "$233.45" in out.content[-1]["text"]


@pytest.mark.asyncio
async def test_noop_without_watchlist():
    mw = _mw()
    p1, p2, p3, p4 = _patched([])
    with p1, p2, p3, p4:
        assert await mw.abefore_model(_human_state(), _runtime(), config=_CFG) is None


@pytest.mark.asyncio
async def test_noop_without_thread_id():
    mw = _mw()
    result = await mw.abefore_model(
        _human_state(), _runtime(), config={"configurable": {}}
    )
    assert result is None


@pytest.mark.asyncio
async def test_throttle_blocks_second_injection():
    mw = _mw(interval=9999)
    p1, p2, p3, p4 = _patched(["NVDA"])
    with p1, p2, p3, p4:
        assert await mw.abefore_model(_human_state(), _runtime(), config=_CFG) is not None
        assert await mw.abefore_model(_human_state(), _runtime(), config=_CFG) is None


@pytest.mark.asyncio
async def test_skips_when_all_venues_closed():
    # Venue gate: every watched symbol's exchange is CLOSED → inject nothing.
    mw = _mw()
    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(return_value=_SNAPS)
    with patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=["NVDA"])), \
         patch(f"{_MOD}.get_market_data_provider", return_value=provider), \
         patch(f"{_MOD}.get_calendar", return_value=_fake_calendar(MarketPhase.CLOSED)):
        assert await mw.abefore_model(_human_state(), _runtime(), config=_CFG) is None


@pytest.mark.asyncio
async def test_stamps_when_hk_open_while_us_closed():
    # Deferred-bug fix: a watchlist mixing a closed US name and an open HK name
    # must still stamp — the gate opens when ANY watched venue is open, priced
    # per-symbol against its own exchange calendar.
    mw = _mw()
    hk_snaps = [{"symbol": "0700.HK", "price": 318.20, "change_percent": 0.5,
                 "volume": 1_000_000, "last_trade_price": 318.20, "market_status": "open"}]
    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(return_value=hk_snaps)

    def _by_venue(calendar_id):
        # XNYS closed, XHKG open.
        return _fake_calendar(
            MarketPhase.REGULAR if calendar_id == "XHKG" else MarketPhase.CLOSED
        )

    with patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=["AAPL", "0700.HK"])), \
         patch(f"{_MOD}.get_market_data_provider", return_value=provider), \
         patch("src.tools.market_data.quote_format.get_market_session",
               return_value=("CLOSED", _FIXED_ET)), \
         patch(f"{_MOD}.get_calendar", side_effect=_by_venue):
        update = await mw.abefore_model(_human_state(), _runtime(), config=_CFG)
    assert update is not None
    assert "<market-watch>" in update["messages"][0].content


@pytest.mark.asyncio
async def test_provider_failure_is_silent():
    mw = _mw()
    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(side_effect=RuntimeError("down"))
    with patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=["NVDA"])), \
         patch(f"{_MOD}.get_market_data_provider", return_value=provider), \
         patch(f"{_MOD}.get_calendar", return_value=_fake_calendar(MarketPhase.REGULAR)):
        assert await mw.abefore_model(_human_state(), _runtime(), config=_CFG) is None


@pytest.mark.asyncio
async def test_post_throttle_failure_is_silent():
    # A raise past the cheap in-memory guards (here the venue-gate calendar) must
    # degrade to injecting nothing — never break the turn.
    mw = _mw()
    with patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=["NVDA"])), \
         patch(f"{_MOD}.get_calendar", side_effect=RuntimeError("boom")):
        assert await mw.abefore_model(_human_state(), _runtime(), config=_CFG) is None


# --- tool-batch carrier selection -------------------------------------------
#
# (Removed: test_stamps_tool_result — the per-tool-result awrap_tool_call path is
# gone; stamping is now a single abefore_model carrier pick. Removed:
# test_throttle_race_single_stamp — abefore_model is the sole, sequential call
# site, so the concurrency lock it exercised no longer exists.)


@pytest.mark.asyncio
async def test_stamps_tool_batch_carrier():
    # Inverted from the old test_skips_when_tail_not_human: a ToolMessage-batch
    # tail must now STAMP the selected carrier instead of returning None.
    mw = _mw()
    assert mw._last_injected_at is None
    tools = [ToolMessage(content="a web search result",
                         tool_call_id="tc-1", name="web_search", id="tm-1")]
    state = _batch_state(tools)
    p1, p2, p3, p4 = _patched(["NVDA"])
    with p1, p2, p3, p4:
        update = await mw.abefore_model(state, _runtime(), config=_CFG)
    assert update is not None
    out = update["messages"][0]
    assert out.id == "tm-1"
    assert "a web search result" in out.content
    assert "<market-watch>" in out.content
    assert "$233.45" in out.content


@pytest.mark.asyncio
async def test_batch_prefers_todowrite():
    # TodoWrite result is chosen even though it is longer than the other result —
    # priority beats length.
    mw = _mw()
    tools = [
        ToolMessage(content="short", tool_call_id="tc-a", name="web_search", id="tm-a"),
        ToolMessage(content="Todos have been modified successfully. Continue.",
                    tool_call_id="tc-b", name="TodoWrite", id="tm-b"),
    ]
    state = _batch_state(tools)
    p1, p2, p3, p4 = _patched(["NVDA"])
    with p1, p2, p3, p4:
        update = await mw.abefore_model(state, _runtime(), config=_CFG)
    out = update["messages"][0]
    assert out.id == "tm-b"
    assert out.name == "TodoWrite"
    assert out.tool_call_id == "tc-b"
    assert "<market-watch>" in out.content


@pytest.mark.asyncio
async def test_batch_shortest_when_no_todowrite():
    mw = _mw()
    tools = [
        ToolMessage(content="x" * 100, tool_call_id="tc-a", name="web_search", id="tm-long"),
        ToolMessage(content="x" * 10, tool_call_id="tc-b", name="web_fetch", id="tm-short"),
    ]
    state = _batch_state(tools)
    p1, p2, p3, p4 = _patched(["NVDA"])
    with p1, p2, p3, p4:
        update = await mw.abefore_model(state, _runtime(), config=_CFG)
    out = update["messages"][0]
    assert out.id == "tm-short"
    assert "<market-watch>" in out.content


@pytest.mark.asyncio
async def test_batch_skips_errored_shortest():
    # Shortest result is errored → skipped; next shortest successful is chosen.
    mw = _mw()
    tools = [
        ToolMessage(content="err", tool_call_id="tc-a", name="web_search",
                    id="tm-err", status="error"),
        ToolMessage(content="a longer ok result", tool_call_id="tc-b",
                    name="web_fetch", id="tm-ok"),
    ]
    state = _batch_state(tools)
    p1, p2, p3, p4 = _patched(["NVDA"])
    with p1, p2, p3, p4:
        update = await mw.abefore_model(state, _runtime(), config=_CFG)
    out = update["messages"][0]
    assert out.id == "tm-ok"
    assert "<market-watch>" in out.content


@pytest.mark.asyncio
async def test_batch_fallback_to_tail_when_no_string_candidate():
    # No string-content candidate (all list-content) → carrier falls back to tail.
    mw = _mw()
    tools = [
        ToolMessage(content=[{"type": "text", "text": "a"}],
                    tool_call_id="tc-a", name="web_search", id="tm-1"),
        ToolMessage(content=[{"type": "text", "text": "b"}],
                    tool_call_id="tc-b", name="web_search", id="tm-2"),
    ]
    state = _batch_state(tools)
    p1, p2, p3, p4 = _patched(["NVDA"])
    with p1, p2, p3, p4:
        update = await mw.abefore_model(state, _runtime(), config=_CFG)
    out = update["messages"][0]
    assert out.id == "tm-2"  # the tail
    assert isinstance(out.content, list)
    assert out.content[-1]["type"] == "text"
    assert "<market-watch>" in out.content[-1]["text"]


@pytest.mark.asyncio
async def test_skips_when_batch_already_quoted():
    # The AIMessage that made the batch called a quote tool for a watched symbol
    # (case-insensitive) → the model already has a fresh price, so skip injection.
    mw = _mw()
    tools = [ToolMessage(content="NVDA  $230.00", tool_call_id="tc-q",
                         name="get_quote", id="tm-q")]
    tool_calls = [{"name": "get_quote", "args": {"symbols": ["nvda"]}, "id": "tc-q"}]
    state = _batch_state(tools, tool_calls)
    p1, p2, p3, p4 = _patched(["NVDA"])
    with p1, p2, p3, p4:
        assert await mw.abefore_model(state, _runtime(), config=_CFG) is None


@pytest.mark.asyncio
async def test_idempotent_when_carrier_already_stamped():
    mw = _mw()
    tools = [ToolMessage(content="result\n\n<market-watch>\nold\n</market-watch>",
                         tool_call_id="tc-1", name="web_search", id="tm-1")]
    state = _batch_state(tools)
    p1, p2, p3, p4 = _patched(["NVDA"])
    with p1, p2, p3, p4:
        assert await mw.abefore_model(state, _runtime(), config=_CFG) is None


@pytest.mark.asyncio
async def test_stamps_middle_of_batch_carrier():
    # The chosen carrier is the shortest result, which is NOT the tail. The
    # returned message id must equal that carrier's id — proving add_messages
    # updates it in place off the tail.
    mw = _mw()
    tools = [
        ToolMessage(content="short", tool_call_id="tc-a", name="web_search", id="tm-mid"),
        ToolMessage(content="a much longer trailing result", tool_call_id="tc-b",
                    name="web_fetch", id="tm-tail"),
    ]
    state = _batch_state(tools)
    p1, p2, p3, p4 = _patched(["NVDA"])
    with p1, p2, p3, p4:
        update = await mw.abefore_model(state, _runtime(), config=_CFG)
    out = update["messages"][0]
    assert out.id == "tm-mid"  # off-tail carrier updated by id
    assert "<market-watch>" in out.content


@pytest.mark.asyncio
async def test_determinism_lowest_index_on_tie():
    mw = _mw()
    tools = [
        ToolMessage(content="AAAA", tool_call_id="tc-a", name="web_search", id="tm-first"),
        ToolMessage(content="BBBB", tool_call_id="tc-b", name="web_fetch", id="tm-second"),
    ]
    state = _batch_state(tools)
    p1, p2, p3, p4 = _patched(["NVDA"])
    with p1, p2, p3, p4:
        update = await mw.abefore_model(state, _runtime(), config=_CFG)
    out = update["messages"][0]
    assert out.id == "tm-first"  # equal length → lowest batch index wins


@pytest.mark.asyncio
async def test_todo_card_safety_regression():
    # Stamping a TodoWrite result leaves name/tool_call_id/id unchanged and only
    # appends to content. The todo SSE card is built from the tool_call args, not
    # the result content (sse_middleware.py:68-70), so the card is unaffected.
    mw = _mw()
    todo = ToolMessage(content="Todos have been modified successfully.",
                       tool_call_id="tc-td", name="TodoWrite", id="tm-td")
    state = _batch_state([todo])
    p1, p2, p3, p4 = _patched(["NVDA"])
    with p1, p2, p3, p4:
        update = await mw.abefore_model(state, _runtime(), config=_CFG)
    out = update["messages"][0]
    assert out.name == "TodoWrite"
    assert out.tool_call_id == "tc-td"
    assert out.id == "tm-td"
    assert out.content.startswith("Todos have been modified successfully.")
    assert "<market-watch>" in out.content
