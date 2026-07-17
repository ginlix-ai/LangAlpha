"""MarketWatchMiddleware ephemeral injection behavior (awrap_model_call).

The middleware appends one `<market-watch>` HumanMessage to the model request
via ``request.override`` — nothing enters durable state, so there is no
carrier selection, no idempotency guard, and no projector strip to test.
"""

import contextlib
import hashlib
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytz
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.market_watch import MarketWatchMiddleware
from src.market_protocol import MarketPhase


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
    """Minimal ModelRequest stand-in: messages + runtime + model + immutable override."""

    def __init__(self, messages, runtime=None, model=None):
        self.messages = messages
        self.runtime = runtime or MagicMock()
        self.model = model

    def override(self, **overrides):
        return _FakeRequest(
            overrides.get("messages", self.messages),
            runtime=self.runtime,
            model=self.model,
        )


def _fake_calendar(phase):
    """A calendar whose phase_at returns a fixed MarketPhase for the venue gate."""
    cal = MagicMock()
    cal.phase_at = MagicMock(return_value=phase)
    return cal


def _mw(interval=25, **kwargs):
    kwargs.setdefault("cache_breakpoint_pin", True)
    return MarketWatchMiddleware(min_interval_seconds=interval, **kwargs)


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
            patch(f"{_MOD}.get_calendar", return_value=_fake_calendar(phase))
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


def _assert_stamped(original, injected, count=1):
    """Injected request = original messages + `count` ephemeral stamp tails."""
    assert len(injected.messages) == len(original.messages) + count
    assert injected.messages[: len(original.messages)] == original.messages
    tail = injected.messages[-1]
    assert isinstance(tail, HumanMessage)
    assert tail.content.startswith("<market-watch>\n")
    assert tail.content.endswith("\n</market-watch>")
    # Self-identifies as feed output so it can't be mistaken for the user.
    assert "not a user message" in tail.content
    return tail


# --- ephemeral stamp ----------------------------------------------------------


@pytest.mark.asyncio
async def test_appends_ephemeral_stamp_for_human_tail(recording_handler):
    mw = _mw()
    request = _human_request()

    with _patched(["NVDA"]):
        result = await mw.awrap_model_call(request, recording_handler)

    assert result == "MODEL_RESPONSE"
    tail = _assert_stamped(request, recording_handler.seen[0])
    assert "$233.45" in tail.content
    # Nothing persisted: the original request's messages are untouched.
    assert len(request.messages) == 1
    assert request.messages[0].content == "What is NVDA doing?"


@pytest.mark.asyncio
async def test_appends_after_tool_batch_without_touching_results(recording_handler):
    mw = _mw()
    tools = [ToolMessage(content="a web search result",
                         tool_call_id="tc-1", name="web_search", id="tm-1")]
    request = _batch_request(tools)

    with _patched(["NVDA"]):
        await mw.awrap_model_call(request, recording_handler)

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
        result = await mw.awrap_model_call(request, recording_handler)

    assert result == "MODEL_RESPONSE"
    assert recording_handler.seen[0] is request  # untouched request passes through


@pytest.mark.asyncio
async def test_noop_without_thread_id(recording_handler):
    mw = _mw()
    request = _human_request()

    with patch(f"{_MOD}.get_config", MagicMock(return_value={"configurable": {}})):
        await mw.awrap_model_call(request, recording_handler)

    assert recording_handler.seen[0] is request


@pytest.mark.asyncio
async def test_noop_outside_runnable_context(recording_handler):
    # get_config raising (no runnable context) must degrade to a pass-through.
    mw = _mw()
    request = _human_request()

    with patch(f"{_MOD}.get_config", MagicMock(side_effect=RuntimeError("no ctx"))):
        await mw.awrap_model_call(request, recording_handler)

    assert recording_handler.seen[0] is request


# --- throttle -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_throttle_reinjects_cached_block_without_refetch(recording_handler):
    mw = _mw(interval=9999)

    with _patched(["NVDA"]) as ctx:
        first = _human_request()
        await mw.awrap_model_call(first, recording_handler)
        second = _human_request()
        await mw.awrap_model_call(second, recording_handler)

    # Both calls got a stamp, but the provider was only hit once — the second
    # call re-injected the cached block (watchlist unchanged, still in window).
    seen = recording_handler.seen
    tail_1 = _assert_stamped(first, seen[0])
    tail_2 = _assert_stamped(second, seen[1])
    assert tail_1.content == tail_2.content
    assert ctx.provider.get_snapshots.await_count == 1


@pytest.mark.asyncio
async def test_sse_emitted_only_on_fresh_fetch(recording_handler):
    mw = _mw(interval=9999)
    runtime = MagicMock()
    runtime.stream_writer = MagicMock()

    with _patched(["NVDA"]):
        await mw.awrap_model_call(
            _FakeRequest(_human_request().messages, runtime), recording_handler
        )
        await mw.awrap_model_call(
            _FakeRequest(_human_request().messages, runtime), recording_handler
        )

    events = [c.args[0] for c in runtime.stream_writer.call_args_list]
    updates = [e for e in events if e["type"] == "market_watch_update"]
    # The UI update fires only on the fresh fetch; the throttled replay emits
    # provenance only (covered in the provenance section below).
    assert len(updates) == 1
    assert updates[0]["symbols"] == ["NVDA"]
    assert "$233.45" in updates[0]["content"]


# --- watchlist / venue changes mid-throttle-window ----------------------------


@pytest.mark.asyncio
async def test_watchlist_change_mid_window_refetches(recording_handler):
    # A mid-window mutation must not replay the stale block (removed symbols
    # would resurface) nor go silent (a just-watched symbol would be invisible
    # for the rest of the window) — it refetches for the current list.
    mw = _mw(interval=9999)
    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(return_value=_SNAPS)
    with patch(f"{_MOD}.get_watchlist",
               AsyncMock(side_effect=[["NVDA", "TSLA"], ["NVDA"]])), \
         patch(f"{_MOD}.get_market_data_provider", return_value=provider), \
         patch("src.tools.market_data.quote_format.get_market_session",
               return_value=("REGULAR_HOURS", _FIXED_ET)), \
         patch(f"{_MOD}.get_calendar", return_value=_fake_calendar(MarketPhase.REGULAR)), \
         patch(f"{_MOD}.get_config", MagicMock(return_value=_CFG)):
        first = _human_request()
        await mw.awrap_model_call(first, recording_handler)
        second = _human_request()
        await mw.awrap_model_call(second, recording_handler)

    seen = recording_handler.seen
    _assert_stamped(first, seen[0])
    _assert_stamped(second, seen[1])  # changed list → fresh fetch, not replay
    assert provider.get_snapshots.await_count == 2
    # The refetch targets the CURRENT list, not the cached one.
    assert provider.get_snapshots.await_args_list[1].args[0] == ["NVDA"]


@pytest.mark.asyncio
async def test_venue_close_mid_window_stops_injection(recording_handler):
    # A venue that closes mid-window must stop the replay: the venue gate sits
    # above the throttle and is re-evaluated every call.
    mw = _mw(interval=9999)
    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(return_value=_SNAPS)
    with patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=["NVDA"])), \
         patch(f"{_MOD}.get_market_data_provider", return_value=provider), \
         patch("src.tools.market_data.quote_format.get_market_session",
               return_value=("REGULAR_HOURS", _FIXED_ET)), \
         patch(f"{_MOD}.get_calendar",
               side_effect=[_fake_calendar(MarketPhase.REGULAR),
                            _fake_calendar(MarketPhase.CLOSED)]), \
         patch(f"{_MOD}.get_config", MagicMock(return_value=_CFG)):
        first = _human_request()
        await mw.awrap_model_call(first, recording_handler)
        second = _human_request()
        await mw.awrap_model_call(second, recording_handler)

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
        await mw.awrap_model_call(request, recording_handler)

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

    def _by_venue(calendar_id):
        # XNYS closed, XHKG open.
        return _fake_calendar(
            MarketPhase.REGULAR if calendar_id == "XHKG" else MarketPhase.CLOSED
        )

    with patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=["AAPL", "0700.HK"])), \
         patch(f"{_MOD}.get_market_data_provider", return_value=provider), \
         patch("src.tools.market_data.quote_format.get_market_session",
               return_value=("CLOSED", _FIXED_ET)), \
         patch(f"{_MOD}.get_calendar", side_effect=_by_venue), \
         patch(f"{_MOD}.get_config", MagicMock(return_value=_CFG)):
        await mw.awrap_model_call(request, recording_handler)

    _assert_stamped(request, recording_handler.seen[0])


@pytest.mark.asyncio
async def test_provider_failure_is_silent(recording_handler):
    mw = _mw()
    request = _human_request()

    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(side_effect=RuntimeError("down"))
    with patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=["NVDA"])), \
         patch(f"{_MOD}.get_market_data_provider", return_value=provider), \
         patch(f"{_MOD}.get_calendar", return_value=_fake_calendar(MarketPhase.REGULAR)), \
         patch(f"{_MOD}.get_config", MagicMock(return_value=_CFG)):
        result = await mw.awrap_model_call(request, recording_handler)

    assert result == "MODEL_RESPONSE"
    assert recording_handler.seen[0] is request


@pytest.mark.asyncio
async def test_post_throttle_failure_is_silent(recording_handler):
    # A raise past the cheap in-memory guards (here the venue-gate calendar) must
    # degrade to injecting nothing — never break the turn.
    mw = _mw()
    request = _human_request()

    with patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=["NVDA"])), \
         patch(f"{_MOD}.get_calendar", side_effect=RuntimeError("boom")), \
         patch(f"{_MOD}.get_config", MagicMock(return_value=_CFG)):
        result = await mw.awrap_model_call(request, recording_handler)

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
        await mw.awrap_model_call(request, recording_handler)

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
        await mw.awrap_model_call(request, recording_handler)

    _assert_stamped(request, recording_handler.seen[0])


@pytest.mark.asyncio
async def test_sse_symbols_are_watchlist_not_priced_subset(recording_handler):
    # One provider timeout must not make the chip claim the symbol is unwatched:
    # the SSE update carries the authoritative watch list, not the priced subset.
    mw = _mw()
    runtime = MagicMock()
    runtime.stream_writer = MagicMock()

    with _patched(["NVDA", "TSLA"]):  # snaps only price NVDA
        await mw.awrap_model_call(
            _FakeRequest(_human_request().messages, runtime), recording_handler
        )

    events = [c.args[0] for c in runtime.stream_writer.call_args_list]
    updates = [e for e in events if e["type"] == "market_watch_update"]
    assert updates[0]["symbols"] == ["NVDA", "TSLA"]


# --- anthropic cache breakpoint --------------------------------------------------


def _anthropic_model():
    from langchain_anthropic import ChatAnthropic

    return ChatAnthropic(model="claude-sonnet-4-5", api_key="test-key")


def _openai_model(**kwargs):
    from langchain_openai import ChatOpenAI

    return ChatOpenAI(model="gpt-5.6-sol", api_key="test-key", **kwargs)


@pytest.mark.asyncio
async def test_anthropic_durable_tail_gets_cache_breakpoint(recording_handler):
    # The stamp evaporates from the next request, so the moving breakpoint must
    # land on the last durable message — pinned via a request-scoped copy.
    mw = _mw()
    original = HumanMessage(content="What is NVDA doing?", id="h-1")
    request = _FakeRequest([original], model=_anthropic_model())

    with _patched(["NVDA"]):
        await mw.awrap_model_call(request, recording_handler)

    durable = recording_handler.seen[0].messages[-2]
    assert durable is not original
    assert durable.content == [
        {
            "type": "text",
            "text": "What is NVDA doing?",
            "cache_control": {"type": "ephemeral"},
        }
    ]
    # The durable state message itself is never mutated.
    assert original.content == "What is NVDA doing?"
    assert isinstance(recording_handler.seen[0].messages[-1], HumanMessage)


@pytest.mark.asyncio
async def test_non_anthropic_durable_tail_untouched(recording_handler):
    # cache_control is Anthropic wire format; other providers must not see it.
    mw = _mw()
    original = HumanMessage(content="What is NVDA doing?", id="h-1")
    request = _FakeRequest([original], model=MagicMock())

    with _patched(["NVDA"]):
        await mw.awrap_model_call(request, recording_handler)

    assert recording_handler.seen[0].messages[-2] is original


@pytest.mark.asyncio
async def test_cache_breakpoint_tags_last_text_block_of_list_content(recording_handler):
    mw = _mw()
    tool = ToolMessage(
        content=[{"type": "text", "text": "part 1"}, {"type": "text", "text": "part 2"}],
        tool_call_id="tc-1", name="web_search", id="tm-1",
    )
    ai = AIMessage(content="", tool_calls=[], id="ai-1")
    request = _FakeRequest(
        [HumanMessage(content="hi", id="h-0"), ai, tool], model=_anthropic_model()
    )

    with _patched(["NVDA"]):
        await mw.awrap_model_call(request, recording_handler)

    tagged = recording_handler.seen[0].messages[-2]
    assert tagged.content[0] == {"type": "text", "text": "part 1"}
    assert tagged.content[1] == {
        "type": "text", "text": "part 2", "cache_control": {"type": "ephemeral"}
    }
    assert "cache_control" not in tool.content[1]


@pytest.mark.asyncio
async def test_cache_breakpoint_skips_non_text_tail_block(recording_handler):
    # A durable message can end in a non-text block (image attachment); the
    # marker must land on the last TEXT block, leaving the tail untouched —
    # some providers reject cache markers on non-text blocks.
    mw = _mw()
    image_block = {"type": "image", "source": {"type": "base64", "data": "xx"}}
    tool = ToolMessage(
        content=[{"type": "text", "text": "part 1"}, image_block],
        tool_call_id="tc-1", name="web_search", id="tm-1",
    )
    ai = AIMessage(content="", tool_calls=[], id="ai-1")
    request = _FakeRequest(
        [HumanMessage(content="hi", id="h-0"), ai, tool], model=_anthropic_model()
    )

    with _patched(["NVDA"]):
        await mw.awrap_model_call(request, recording_handler)

    tagged = recording_handler.seen[0].messages[-2]
    assert tagged.content[0] == {
        "type": "text", "text": "part 1", "cache_control": {"type": "ephemeral"}
    }
    assert tagged.content[1] == image_block
    assert "cache_control" not in image_block


@pytest.mark.asyncio
async def test_cache_breakpoint_skips_untaggable_tail(recording_handler):
    # An empty tool result has no block that accepts cache_control — degrade to
    # no breakpoint (today's cache behavior), never a malformed request.
    mw = _mw()
    tool = ToolMessage(content="", tool_call_id="tc-1", name="web_search", id="tm-1")
    ai = AIMessage(content="", tool_calls=[], id="ai-1")
    request = _FakeRequest(
        [HumanMessage(content="hi", id="h-0"), ai, tool], model=_anthropic_model()
    )

    with _patched(["NVDA"]):
        await mw.awrap_model_call(request, recording_handler)

    assert recording_handler.seen[0].messages[-2] is tool


@pytest.mark.asyncio
async def test_openai_official_with_cache_options_gets_breakpoint(recording_handler):
    # OpenAI's explicit mode keys reads at provided breakpoints only, so the
    # durable tail needs the same pin — with the OpenAI marker, not Anthropic's.
    mw = _mw()
    original = HumanMessage(content="What is NVDA doing?", id="h-1")
    request = _FakeRequest(
        [original],
        model=_openai_model(
            prompt_cache_options={"mode": "implicit"},
            base_url="https://api.openai.com/v1",
        ),
    )

    with _patched(["NVDA"]):
        await mw.awrap_model_call(request, recording_handler)

    durable = recording_handler.seen[0].messages[-2]
    assert durable is not original
    assert durable.content == [
        {
            "type": "text",
            "text": "What is NVDA doing?",
            "prompt_cache_breakpoint": {"mode": "explicit"},
        }
    ]
    assert original.content == "What is NVDA doing?"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "model_kwargs",
    [
        {},  # no prompt_cache_options opt-in
        {  # opted in, but non-official endpoint (proxy) rejects the marker
            "prompt_cache_options": {"mode": "implicit"},
            "base_url": "https://proxy.example.com/v1",
        },
    ],
)
async def test_openai_without_optin_or_official_endpoint_untouched(
    model_kwargs, recording_handler
):
    mw = _mw()
    original = HumanMessage(content="What is NVDA doing?", id="h-1")
    request = _FakeRequest([original], model=_openai_model(**model_kwargs))

    with _patched(["NVDA"]):
        await mw.awrap_model_call(request, recording_handler)

    assert recording_handler.seen[0].messages[-2] is original


@pytest.mark.asyncio
async def test_cache_pin_flag_off_leaves_durable_tail_untouched(recording_handler):
    mw = _mw(cache_breakpoint_pin=False)
    original = HumanMessage(content="What is NVDA doing?", id="h-1")
    request = _FakeRequest([original], model=_anthropic_model())

    with _patched(["NVDA"]):
        await mw.awrap_model_call(request, recording_handler)

    # Stamp still appended; only the breakpoint pin is disabled.
    seen = recording_handler.seen
    assert seen[0].messages[-2] is original
    assert isinstance(seen[0].messages[-1], HumanMessage)
    assert seen[0].messages[-1].content.startswith("<market-watch>")


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
        await mw.awrap_model_call(request, recording_handler)

    prov = _events(runtime, "provenance")
    assert [(e["identifier"], e["provider"]) for e in prov] == [
        ("NVDA", "ginlix-data"),
        ("TSLA", "market_data_proxy"),  # no per-snap source → generic fallback
    ]
    stamp = recording_handler.seen[0].messages[-1].content
    expected_sha = hashlib.sha256(stamp.encode("utf-8")).hexdigest()
    for e in prov:
        assert e["source_type"] == "market_data"
        assert e["detail"] == "market_watch"
        assert e["tool_call_id"] is None
        # The record attests the exact bytes the model saw: the wrapped stamp.
        assert e["result_sha256"] == expected_sha
        assert e["result_snippet"].startswith("<market-watch>")


@pytest.mark.asyncio
async def test_provenance_replayed_stamp_reattested_with_fetch_timestamp(recording_handler):
    # Ephemeral replays re-display the cached block, so every model call must
    # attest it — same sha and same (original) fetch timestamp both times.
    mw = _mw(interval=9999)
    runtime, _ = _runtime_request()

    with _patched(["NVDA"]) as ctx:
        await mw.awrap_model_call(
            _FakeRequest(_human_request().messages, runtime), recording_handler
        )
        await mw.awrap_model_call(
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
        await mw.awrap_model_call(
            _FakeRequest(_human_request().messages, runtime), recording_handler
        )
        await mw.awrap_model_call(
            _FakeRequest(_human_request().messages, runtime), recording_handler
        )
        await mw.aafter_agent(None, runtime)  # drains the background write

    stamp = recording_handler.seen[0].messages[-1].content
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
        result = await mw.awrap_model_call(request, recording_handler)

    assert result == "MODEL_RESPONSE"
    _assert_stamped(request, recording_handler.seen[0])
