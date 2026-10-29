"""MarketWatchMiddleware row behavior (abefore_model).

The middleware writes one durable ``market_watch`` row into history before each
model call it stamps. The carrier owns which stamps the model reads, their
shape and the cache breakpoint; none of that is this middleware's concern.
"""

import contextlib
import hashlib
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
import pytz
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.market_watch import MarketWatchMiddleware, _any_venue_open
from ptc_agent.agent.middleware.runtime_context import (
    is_runtime_update_message,
    runtime_update_from_message,
)
from market_protocol import MarketPhase


@pytest.fixture(autouse=True)
def body_store(monkeypatch):
    """Silence the lazy body-store write (no DB in unit tests); assertable."""
    mock = AsyncMock()
    monkeypatch.setattr(
        "src.server.database.provenance_bodies.store_result_body", mock
    )
    return mock


@pytest.fixture
def recording_handler():
    """Model-call handler that records each request it sees and returns a sentinel."""

    seen = []

    async def handler(req):
        seen.append(req)
        return "MODEL_RESPONSE"

    handler.seen = seen
    return handler


_MOD = "ptc_agent.agent.middleware.market_watch"
_ET = pytz.timezone("US/Eastern")
_FIXED_ET = _ET.localize(datetime(2026, 7, 1, 14, 32, 5))
_CFG = {"configurable": {"thread_id": "t-1", "user_id": "u-1"}}

_SNAPS = [{"symbol": "NVDA", "price": 231.0, "change_percent": 2.31,
           "volume": 1_000_000, "last_trade_price": 233.45, "market_status": "open"}]


class _FakeRequest:
    """Minimal ModelRequest stand-in: messages + state + runtime + model + override."""

    def __init__(self, messages, runtime=None, model=None, state=None):
        self.messages = messages
        self.runtime = runtime or MagicMock()
        self.model = model
        self.state = state if state is not None else {"messages": messages}

    def override(self, **overrides):
        return _FakeRequest(
            overrides.get("messages", self.messages),
            runtime=self.runtime,
            model=self.model,
            state=overrides.get("state", self.state),
        )


async def _call(mw, request, handler):
    """One model call as the agent runs it: the row hook, then the model."""
    update = await mw.abefore_model(request.state, request.runtime)
    if update:
        messages = [*request.messages, *update["messages"]]
        request = request.override(messages=messages, state={**request.state, "messages": messages})
    return await handler(request)


def _stamp_of(request):
    """The wrapped ``<market-watch>`` block inside the newest row a request carries."""
    text = runtime_update_from_message(request.messages[-1]).text
    return text[text.index("<market-watch>") : text.index("</market-watch>") + len("</market-watch>")]


def _fake_clock(phase):
    """An instrument clock whose phase is a fixed MarketPhase for the venue gate."""
    clock = MagicMock()
    clock.phase = MagicMock(return_value=phase)
    return clock


def _mw(interval=25, guidance="detailed"):
    return MarketWatchMiddleware(min_interval_seconds=interval, guidance=guidance)


@contextlib.contextmanager
def _patched(watchlist, snaps=_SNAPS, phase=MarketPhase.REGULAR, cfg=_CFG):
    """Patch the middleware's collaborators for one test; yields the provider mock.

    A single context manager over the five patch points (watchlist read, provider
    factory, market session, exchange calendar, run config) so tests read
    ``with _patched([...]) as ctx: ... ctx.provider.get_snapshots``.
    """
    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(return_value=snaps)
    with contextlib.ExitStack() as stack:
        stack.enter_context(
            patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=watchlist))
        )
        stack.enter_context(
            patch(f"{_MOD}.get_market_data_provider", return_value=provider)
        )
        stack.enter_context(
            patch("src.tools.market_data.quote_format.get_market_session",
                  return_value=("REGULAR_HOURS", _FIXED_ET))
        )
        stack.enter_context(
            patch(f"{_MOD}.clock_for_ref", return_value=_fake_clock(phase))
        )
        stack.enter_context(patch(f"{_MOD}.get_config", MagicMock(return_value=cfg)))
        yield SimpleNamespace(provider=provider)


def _human_request():
    """Request whose tail is the incoming human message."""
    return _FakeRequest([HumanMessage(content="What is NVDA doing?", id="h-1")])


def _batch_request(tool_msgs, tool_calls=None):
    """Request whose tail is a just-completed tool batch (Human → AI → tools)."""
    ai = AIMessage(content="", tool_calls=tool_calls or [], id="ai-1")
    return _FakeRequest([HumanMessage(content="hi", id="h-0"), ai, *tool_msgs])


def _assert_stamped(original, injected):
    """Injected request = the same messages, plus one durable row after them.

    Returns the row wrapped so ``.content`` reads like the stamp text the
    assertions downstream care about.
    """
    assert injected.messages[:-1] == original.messages
    message = injected.messages[-1]
    assert is_runtime_update_message(message)
    row = runtime_update_from_message(message)
    assert row.kind == "market_watch"
    assert row.schema_version == 2
    assert "<market-watch>\n" in row.text
    assert "\n</market-watch>" in row.text
    # Self-identifies as feed output so it can't be mistaken for the user.
    assert "not a user message" in row.text
    # Tagged so a reader can attribute the row without re-parsing its body.
    assert row.provenance["source"] == "runtime_update"
    assert row.provenance["writer"] == "market_watch"
    return SimpleNamespace(content=row.text, row=row)


# --- ephemeral stamp ----------------------------------------------------------


@pytest.mark.asyncio
async def test_writes_stamp_row_for_human_tail(recording_handler):
    mw = _mw()
    request = _human_request()

    with _patched(["NVDA"]):
        result = await _call(mw, request, recording_handler)

    assert result == "MODEL_RESPONSE"
    tail = _assert_stamped(request, recording_handler.seen[0])
    assert "$233.45" in tail.content
    # The row is a new message; the user's own message is untouched.
    assert len(request.messages) == 1
    assert request.messages[0].content == "What is NVDA doing?"


@pytest.mark.asyncio
async def test_appends_after_tool_batch_without_touching_results(recording_handler):
    mw = _mw()
    tools = [ToolMessage(content="a web search result",
                         tool_call_id="tc-1", name="web_search", id="tm-1")]
    request = _batch_request(tools)

    with _patched(["NVDA"]):
        await _call(mw, request, recording_handler)

    seen = recording_handler.seen
    _assert_stamped(request, seen[0])
    # Tool results pass through byte-identical — no carrier mutation.
    assert seen[0].messages[2] is tools[0]
    assert tools[0].content == "a web search result"


@pytest.mark.asyncio
async def test_noop_without_watchlist(recording_handler):
    mw = _mw()
    request = _human_request()

    with _patched([]):
        result = await _call(mw, request, recording_handler)

    assert result == "MODEL_RESPONSE"
    assert recording_handler.seen[0] is request  # untouched request passes through


@pytest.mark.asyncio
async def test_noop_without_thread_id(recording_handler):
    mw = _mw()
    request = _human_request()

    with patch(f"{_MOD}.get_config", MagicMock(return_value={"configurable": {}})):
        await _call(mw, request, recording_handler)

    assert recording_handler.seen[0] is request


@pytest.mark.asyncio
async def test_noop_outside_runnable_context(recording_handler):
    # get_config raising (no runnable context) must degrade to a pass-through.
    mw = _mw()
    request = _human_request()

    with patch(f"{_MOD}.get_config", MagicMock(side_effect=RuntimeError("no ctx"))):
        await _call(mw, request, recording_handler)

    assert recording_handler.seen[0] is request


# --- throttle -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_throttle_restamps_cached_block_without_refetch(recording_handler):
    mw = _mw(interval=9999)

    with _patched(["NVDA"]) as ctx:
        first = _human_request()
        await _call(mw, first, recording_handler)
        second = _human_request()
        await _call(mw, second, recording_handler)

    # Both calls got a stamp, but the provider was only hit once — the second
    # call re-stamped the cached block (watchlist unchanged, still in window).
    seen = recording_handler.seen
    tail_1 = _assert_stamped(first, seen[0])
    tail_2 = _assert_stamped(second, seen[1])
    assert tail_1.content == tail_2.content
    assert ctx.provider.get_snapshots.await_count == 1


@pytest.mark.asyncio
async def test_a_later_call_stamps_again_and_keeps_the_earlier_stamp(recording_handler):
    mw = _mw(interval=0)
    tools = [ToolMessage(content="ok", tool_call_id="tc-1", name="web_search", id="tm-1")]

    with _patched(["NVDA"]):
        await _call(mw, _human_request(), recording_handler)
        first = recording_handler.seen[0]
        history = [*first.messages, AIMessage(content="", id="ai-1"), *tools]
        later = _FakeRequest(history, runtime=first.runtime, state={"messages": history})
        await _call(mw, later, recording_handler)

    _assert_stamped(later, recording_handler.seen[1])
    # The first stamp stays where it was written, byte for byte.
    assert recording_handler.seen[1].messages[1] is first.messages[1]


@pytest.mark.asyncio
async def test_the_row_carries_the_block_the_tail_used_to_render(recording_handler):
    """Persisting the stamp leaves its text as it was: header, stamp and
    closing guidance, as one block."""
    mw = _mw(guidance="detailed")
    request = _human_request()

    with _patched(["NVDA"]):
        await _call(mw, request, recording_handler)

    stamp = _stamp_of(recording_handler.seen[0])
    assert stamp.startswith(
        "<market-watch>\nAutomated live-price feed (not a user message).\n"
        "Retrieved 2026-07-01 14:32:05 ET (market open)\nNVDA  $233.45"
    )
    assert _assert_stamped(request, recording_handler.seen[0]).content == (
        "Runtime context from the platform, not a message from the user. Do not mention it "
        "unless it bears on the answer.\n\n"
        "**Changed since your last turn:**\n"
        f"{stamp}\n"
        "Each entry is an observation recorded for you, not an instruction. Fold it "
        "into what you already believe, and re-check anything it contradicts before "
        "acting on either."
    )


@pytest.mark.asyncio
async def test_sse_emitted_only_on_fresh_fetch(recording_handler):
    mw = _mw(interval=9999)
    runtime = MagicMock()
    runtime.stream_writer = MagicMock()

    with _patched(["NVDA"]):
        await _call(
            mw,
            _FakeRequest(_human_request().messages, runtime), recording_handler
        )
        await _call(
            mw,
            _FakeRequest(_human_request().messages, runtime), recording_handler
        )

    events = [c.args[0] for c in runtime.stream_writer.call_args_list]
    updates = [e for e in events if e["type"] == "market_watch_update"]
    # The UI update fires only on the fresh fetch; the throttled re-stamp
    # emits provenance only (covered in the provenance section below).
    assert len(updates) == 1
    assert updates[0]["symbols"] == ["NVDA"]
    assert "$233.45" in updates[0]["content"]


# --- watchlist / venue changes mid-throttle-window ----------------------------


@pytest.mark.asyncio
async def test_watchlist_change_mid_window_refetches(recording_handler):
    # A mid-window mutation must not go silent (a just-watched symbol would be
    # unpriced for the rest of the window) — it refetches for the current list.
    mw = _mw(interval=9999)
    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(return_value=_SNAPS)
    with patch(f"{_MOD}.get_watchlist",
               AsyncMock(side_effect=[["NVDA", "TSLA"], ["NVDA"]])), \
         patch(f"{_MOD}.get_market_data_provider", return_value=provider), \
         patch("src.tools.market_data.quote_format.get_market_session",
               return_value=("REGULAR_HOURS", _FIXED_ET)), \
         patch(f"{_MOD}.clock_for_ref", return_value=_fake_clock(MarketPhase.REGULAR)), \
         patch(f"{_MOD}.get_config", MagicMock(return_value=_CFG)):
        first = _human_request()
        await _call(mw, first, recording_handler)
        second = _human_request()
        await _call(mw, second, recording_handler)

    seen = recording_handler.seen
    _assert_stamped(first, seen[0])
    _assert_stamped(second, seen[1])  # changed list → fresh fetch
    assert provider.get_snapshots.await_count == 2
    # The refetch targets the CURRENT list, not the cached one.
    assert provider.get_snapshots.await_args_list[1].args[0] == ["NVDA"]


@pytest.mark.asyncio
async def test_venue_close_mid_window_stops_injection(recording_handler):
    # A venue that closes mid-window must stop the stamps: the venue gate sits
    # above the throttle and is re-evaluated every call.
    mw = _mw(interval=9999)
    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(return_value=_SNAPS)
    with patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=["NVDA"])), \
         patch(f"{_MOD}.get_market_data_provider", return_value=provider), \
         patch("src.tools.market_data.quote_format.get_market_session",
               return_value=("REGULAR_HOURS", _FIXED_ET)), \
         patch(f"{_MOD}.clock_for_ref",
               side_effect=[_fake_clock(MarketPhase.REGULAR),
                            _fake_clock(MarketPhase.CLOSED)]), \
         patch(f"{_MOD}.get_config", MagicMock(return_value=_CFG)):
        first = _human_request()
        await _call(mw, first, recording_handler)
        second = _human_request()
        await _call(mw, second, recording_handler)

    seen = recording_handler.seen
    _assert_stamped(first, seen[0])
    assert seen[1] is second  # venue closed within window → nothing injected
    assert provider.get_snapshots.await_count == 1


# --- gates and degradation ------------------------------------------------------


@pytest.mark.asyncio
async def test_skips_when_all_venues_closed(recording_handler):
    # Venue gate: every watched symbol's exchange is CLOSED → inject nothing.
    mw = _mw()
    request = _human_request()

    with _patched(["NVDA"], phase=MarketPhase.CLOSED):
        await _call(mw, request, recording_handler)

    assert recording_handler.seen[0] is request


@pytest.mark.asyncio
async def test_stamps_when_hk_open_while_us_closed(recording_handler):
    # Deferred-bug fix: a watchlist mixing a closed US name and an open HK name
    # must still stamp — the gate opens when ANY watched venue is open, priced
    # per-symbol against its own exchange calendar.
    mw = _mw()
    hk_snaps = [{"symbol": "0700.HK", "price": 318.20, "change_percent": 0.5,
                 "volume": 1_000_000, "last_trade_price": 318.20, "market_status": "open"}]
    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(return_value=hk_snaps)
    request = _human_request()

    def _by_venue(ref):
        # XNYS closed, XHKG open.
        return _fake_clock(
            MarketPhase.REGULAR if ref.calendar_id == "XHKG" else MarketPhase.CLOSED
        )

    with patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=["AAPL", "0700.HK"])), \
         patch(f"{_MOD}.get_market_data_provider", return_value=provider), \
         patch("src.tools.market_data.quote_format.get_market_session",
               return_value=("CLOSED", _FIXED_ET)), \
         patch(f"{_MOD}.clock_for_ref", side_effect=_by_venue), \
         patch(f"{_MOD}.get_config", MagicMock(return_value=_CFG)):
        await _call(mw, request, recording_handler)

    _assert_stamped(request, recording_handler.seen[0])


@pytest.mark.parametrize(
    "symbol,at,expected",
    [
        # A lunch break holds the morning close: nothing to stamp.
        ("0700.HK", datetime(2026, 7, 2, 12, 30, tzinfo=ZoneInfo("Asia/Hong_Kong")), False),
        ("0700.HK", datetime(2026, 7, 2, 10, 30, tzinfo=ZoneInfo("Asia/Hong_Kong")), True),
        # After-hours moves a stock but not an index, which settled at 16:00.
        ("AAPL", datetime(2026, 10, 1, 17, 30, tzinfo=ZoneInfo("America/New_York")), True),
        ("^GSPC", datetime(2026, 10, 1, 17, 30, tzinfo=ZoneInfo("America/New_York")), False),
    ],
)
def test_venue_gate_reads_each_instrument_clock(symbol, at, expected):
    with patch(f"{_MOD}.datetime") as clock:
        clock.now.return_value = at
        assert _any_venue_open([symbol]) is expected


@pytest.mark.asyncio
async def test_provider_failure_is_silent(recording_handler):
    mw = _mw()
    request = _human_request()

    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(side_effect=RuntimeError("down"))
    with patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=["NVDA"])), \
         patch(f"{_MOD}.get_market_data_provider", return_value=provider), \
         patch(f"{_MOD}.clock_for_ref", return_value=_fake_clock(MarketPhase.REGULAR)), \
         patch(f"{_MOD}.get_config", MagicMock(return_value=_CFG)):
        result = await _call(mw, request, recording_handler)

    assert result == "MODEL_RESPONSE"
    assert recording_handler.seen[0] is request


@pytest.mark.asyncio
async def test_post_throttle_failure_is_silent(recording_handler):
    # A raise past the cheap in-memory guards (here the venue-gate calendar) must
    # degrade to injecting nothing — never break the turn.
    mw = _mw()
    request = _human_request()

    with patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=["NVDA"])), \
         patch(f"{_MOD}.clock_for_ref", side_effect=RuntimeError("boom")), \
         patch(f"{_MOD}.get_config", MagicMock(return_value=_CFG)):
        result = await _call(mw, request, recording_handler)

    assert result == "MODEL_RESPONSE"
    assert recording_handler.seen[0] is request


@pytest.mark.asyncio
async def test_skips_when_batch_already_quoted(recording_handler):
    # The AIMessage that made the batch called a quote tool for a watched symbol
    # (case-insensitive) → the model already has a fresh price, so skip injection.
    mw = _mw()
    tools = [ToolMessage(content="NVDA  $230.00", tool_call_id="tc-q",
                         name="get_quote", id="tm-q")]
    tool_calls = [{"name": "get_quote", "args": {"symbols": ["nvda"]}, "id": "tc-q"}]
    request = _batch_request(tools, tool_calls)

    with _patched(["NVDA"]):
        await _call(mw, request, recording_handler)

    assert recording_handler.seen[0] is request


@pytest.mark.asyncio
async def test_partial_quote_coverage_still_injects(recording_handler):
    # Quoting only ONE of two watched symbols must not suppress the stamp —
    # the un-quoted symbol would silently go stale for that model call.
    mw = _mw()
    tools = [ToolMessage(content="NVDA  $230.00", tool_call_id="tc-q",
                         name="get_quote", id="tm-q")]
    tool_calls = [{"name": "get_quote", "args": {"symbols": ["nvda"]}, "id": "tc-q"}]
    request = _batch_request(tools, tool_calls)

    with _patched(["NVDA", "TSLA"]):
        await _call(mw, request, recording_handler)

    _assert_stamped(request, recording_handler.seen[0])


@pytest.mark.asyncio
async def test_sse_symbols_are_watchlist_not_priced_subset(recording_handler):
    # One provider timeout must not make the chip claim the symbol is unwatched:
    # the SSE update carries the authoritative watch list, not the priced subset.
    mw = _mw()
    runtime = MagicMock()
    runtime.stream_writer = MagicMock()

    with _patched(["NVDA", "TSLA"]):  # snaps only price NVDA
        await _call(
            mw,
            _FakeRequest(_human_request().messages, runtime), recording_handler
        )

    events = [c.args[0] for c in runtime.stream_writer.call_args_list]
    updates = [e for e in events if e["type"] == "market_watch_update"]
    assert updates[0]["symbols"] == ["NVDA", "TSLA"]


# --- provenance ---------------------------------------------------------------


def _events(runtime, type_):
    return [c.args[0] for c in runtime.stream_writer.call_args_list
            if c.args[0]["type"] == type_]


def _runtime_request():
    runtime = MagicMock()
    runtime.stream_writer = MagicMock()
    return runtime, _FakeRequest(_human_request().messages, runtime)


@pytest.mark.asyncio
async def test_provenance_emitted_per_symbol_with_provider_attribution(recording_handler):
    mw = _mw()
    runtime, request = _runtime_request()

    snaps = [
        {**_SNAPS[0], "source": "ginlix-data"},
        {"symbol": "TSLA", "price": 310.0, "change_percent": -1.2,
         "volume": 900_000, "last_trade_price": 309.10, "market_status": "open"},
    ]
    with _patched(["NVDA", "TSLA"], snaps=snaps):
        await _call(mw, request, recording_handler)

    prov = _events(runtime, "provenance")
    assert [(e["identifier"], e["provider"]) for e in prov] == [
        ("NVDA", "ginlix-data"),
        ("TSLA", "market_data_proxy"),  # no per-snap source → generic fallback
    ]
    stamp = _stamp_of(recording_handler.seen[0])
    expected_sha = hashlib.sha256(stamp.encode("utf-8")).hexdigest()
    for e in prov:
        assert e["source_type"] == "market_data"
        assert e["detail"] == "market_watch"
        assert e["tool_call_id"] is None
        # The record attests the exact bytes the model saw: the wrapped stamp.
        assert e["result_sha256"] == expected_sha
        assert e["result_snippet"].startswith("<market-watch>")


@pytest.mark.asyncio
async def test_provenance_restamp_reattested_with_fetch_timestamp(recording_handler):
    # A throttled re-stamp sends the cached block again, so every model call
    # attests it — same sha and same (original) fetch timestamp both times.
    mw = _mw(interval=9999)
    runtime, _ = _runtime_request()

    with _patched(["NVDA"]) as ctx:
        await _call(
            mw,
            _FakeRequest(_human_request().messages, runtime), recording_handler
        )
        await _call(
            mw,
            _FakeRequest(_human_request().messages, runtime), recording_handler
        )

    prov = _events(runtime, "provenance")
    assert len(prov) == 2
    assert prov[0]["result_sha256"] == prov[1]["result_sha256"]
    assert prov[0]["timestamp"] == prov[1]["timestamp"]
    assert ctx.provider.get_snapshots.await_count == 1


@pytest.mark.asyncio
async def test_provenance_body_stored_once_per_block(body_store, recording_handler):
    mw = _mw(interval=9999)
    runtime, _ = _runtime_request()

    with _patched(["NVDA"]):
        await _call(
            mw,
            _FakeRequest(_human_request().messages, runtime), recording_handler
        )
        await _call(
            mw,
            _FakeRequest(_human_request().messages, runtime), recording_handler
        )
        await mw.aafter_agent(None, runtime)  # drains the background write

    stamp = _stamp_of(recording_handler.seen[0])
    body_store.assert_awaited_once_with(
        hashlib.sha256(stamp.encode("utf-8")).hexdigest(),
        stamp,
        len(stamp.encode("utf-8")),
        "text/plain; charset=utf-8",
    )


@pytest.mark.asyncio
async def test_provenance_failure_never_blocks_injection(recording_handler):
    mw = _mw()
    runtime = MagicMock()
    runtime.stream_writer = MagicMock(side_effect=RuntimeError("no stream"))
    request = _FakeRequest(_human_request().messages, runtime)

    with _patched(["NVDA"]):
        result = await _call(mw, request, recording_handler)

    assert result == "MODEL_RESPONSE"
    _assert_stamped(request, recording_handler.seen[0])
