"""Preserved thinking survives a thread with an active market watch.

Drives a real agent graph with the market-watch writer and the tail carrier
through two turns while the watched quote moves, and checks the Anthropic
payloads: every thinking block an earlier call produced must be sent again
with a byte-identical prefix, while each call still carries a fresh stamp.
"""

from __future__ import annotations

import contextlib
import json
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest
import pytz
from langchain.agents import create_agent
from langchain_core.messages import HumanMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from ptc_agent.agent.middleware.market_watch import MarketWatchMiddleware
from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
from ptc_agent.agent.middleware.runtime_context.tail import RowCarrierMiddleware
from ptc_agent.agent.prompts import resolve_prompt_guidance
from tests.unit.ptc_agent.agent.prefix_stability import (
    Wire,
    all_breaks,
    anthropic_model,
    reply,
    thinking_count,
)

_MOD = "ptc_agent.agent.middleware.market_watch"
_ET = pytz.timezone("US/Eastern")


@tool
def get_data(query: str) -> str:
    """Look something up."""
    return f"data for {query}"


def _snaps(price: float) -> list[dict]:
    return [{"symbol": "NVDA", "price": price, "change_percent": 1.0, "volume": 1_000_000,
             "last_trade_price": price, "market_status": "open"}]


@contextlib.contextmanager
def _market(prices: list[float]):
    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(side_effect=[_snaps(p) for p in prices])
    clock = iter(_ET.localize(datetime(2026, 7, 1, 14, 30, s)) for s in range(0, 60, 5))
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch(f"{_MOD}.get_watchlist", AsyncMock(return_value=["NVDA"])))
        stack.enter_context(patch(f"{_MOD}.get_market_data_provider", return_value=provider))
        stack.enter_context(patch(f"{_MOD}._any_venue_open", return_value=True))
        stack.enter_context(
            patch(
                "src.tools.market_data.quote_format.get_market_session",
                side_effect=lambda *_a, **_k: ("REGULAR_HOURS", next(clock)),
            )
        )
        stack.enter_context(
            patch("src.server.database.provenance_bodies.store_result_body", AsyncMock())
        )
        yield provider


def _agent(model_name: str, checkpointer: InMemorySaver):
    return create_agent(
        model=anthropic_model(model_name),
        tools=[get_data],
        system_prompt="You are a test agent.",
        middleware=[
            MarketWatchMiddleware(min_interval_seconds=0, guidance=resolve_prompt_guidance()),
            RowCarrierMiddleware(),
            ReasoningCompatibilityMiddleware(),
        ],
        checkpointer=checkpointer,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("model_name", ["claude-opus-5-5", "claude-opus-5-5-oauth"])
async def test_market_watch_keeps_every_earlier_thinking_prefix(monkeypatch, model_name):
    wire = Wire(
        [
            reply(1, tool=("get_data", {"query": "a"})),
            reply(2, tool=("get_data", {"query": "b"})),
            reply(3, text="NVDA is up."),
            reply(4, tool=("get_data", {"query": "c"})),
            reply(5, text="Still up."),
        ]
    ).install(monkeypatch)
    cfg = {"configurable": {"thread_id": "t-prefix", "user_id": "u-1"}}
    # Calls 1-5: the quote moves, holds on call 3, then moves twice more.
    saver = InMemorySaver()
    with _market([231.0, 232.0, 232.0, 233.0, 234.0]):
        # The agent is built per turn, as the server builds it.
        await _agent(model_name, saver).ainvoke({"messages": [HumanMessage("How is NVDA doing?")]}, cfg)
        await _agent(model_name, saver).ainvoke({"messages": [HumanMessage("And now?")]}, cfg)

    assert len(wire.payloads) == 5
    assert [thinking_count(p) for p in wire.payloads] == [0, 1, 2, 3, 4]
    assert all_breaks(wire) == []
    # Every call stamps; each stamp stays as a turn-scoped system message the
    # API clears once a later user message exists.
    for n, payload in enumerate(wire.payloads, start=1):
        stamps = [m for m in payload["messages"] if "<market-watch>" in json.dumps(m)]
        assert len(stamps) == n
        assert all(m["role"] == "system" and m["clear_at"] == "next_user_message" for m in stamps)
    beta = "mid-conversation-system-clear-at-2026-08-21"
    assert beta in anthropic_model(model_name).default_headers["anthropic-beta"]


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
async def test_the_stamp_reaches_the_http_body_turn_scoped(monkeypatch, stream):
    """langchain-anthropic 1.7.6 drops ``clear_at`` from a system message it
    formats itself; the operator channel formats the stamp instead, and the
    key has to reach the HTTP body the SDK sends."""
    wire = Wire(
        [reply(1, tool=("get_data", {"query": "a"})), reply(2, text="NVDA is up.")]
    ).install(monkeypatch, stream=stream)
    cfg = {"configurable": {"thread_id": "t-body", "user_id": "u-1"}}
    with _market([231.0, 232.0]):
        await _agent("claude-sonnet-5-5", InMemorySaver()).ainvoke(
            {"messages": [HumanMessage("How is NVDA doing?")]}, cfg
        )

    assert len(wire.bodies) == 2
    for n, body in enumerate(wire.bodies, start=1):
        stamps = [m for m in body["messages"] if "<market-watch>" in json.dumps(m)]
        assert len(stamps) == n
        assert all(m["role"] == "system" and m["clear_at"] == "next_user_message" for m in stamps)
