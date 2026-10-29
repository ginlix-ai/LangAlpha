"""Claude Fable 5.1 drops a block whose prefix changed instead of refusing the request.

Where the API enforces the prefix check, a request with ``thinking.block_binding``
unset is refused with a 400 on the first edit, and the resilience retry then
strips every block. Both Fable routes send ``prefix_mismatch_behavior: "drop_block"``
with the beta it needs, the way the 5.5 entries do.
"""

from __future__ import annotations

import pytest
from langchain.agents import create_agent
from langchain_core.messages import HumanMessage

from tests.unit.ptc_agent.agent.prefix_stability import BINDING_BETA, Wire, anthropic_model, reply


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["claude-fable-5-1", "claude-fable-5-1-oauth"])
async def test_both_fable_routes_drop_a_mismatched_block(monkeypatch, route):
    wire = Wire([reply(1, text="Hello.", model_name="claude-fable-5-1")]).install(monkeypatch, stream=False)
    agent = create_agent(model=anthropic_model(route), system_prompt="Answer plainly.")
    await agent.ainvoke({"messages": [HumanMessage("Hi.")]})

    assert wire.payloads[0]["thinking"]["block_binding"] == {"prefix_mismatch_behavior": "drop_block"}
    assert BINDING_BETA in wire.headers[0].split(",")
    assert wire.transformations == [[]]
