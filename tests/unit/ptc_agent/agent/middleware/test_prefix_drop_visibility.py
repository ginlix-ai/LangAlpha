"""A thinking block the API drops is seen: on the answer, in the log, on the run.

The API lists each earlier thinking block it dropped in the response's
``input_transformations`` (on ``message_start`` when streaming). The Wire answers
over HTTP as the API does, so the list goes through the SDK and
langchain-anthropic's own parsing; it has to reach the answer's
``response_metadata`` as sent, and every answer with a drop is logged with its
thread and counts by reason. The edit here is deliberate: the second turn
changes the system prompt, which fails every earlier block.
"""

from __future__ import annotations

import pytest
from langchain.agents import create_agent
from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from structlog.testing import capture_logs

from ptc_agent.agent.middleware.model_resilience import ModelResilienceMiddleware
from tests.unit.ptc_agent.agent.prefix_stability import Wire, anthropic_model, drops, listed, reply

THREAD = "c3d4e5f6-0000-4000-8000-000000000000"


def _agent(saver: InMemorySaver, system: str):
    model = anthropic_model()
    return create_agent(
        model=model,
        system_prompt=system,
        middleware=[ModelResilienceMiddleware(primary_name="claude-opus-5-5", primary_client=model)],
        checkpointer=saver,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True], ids=["response", "stream"])
async def test_a_dropped_block_reaches_the_answer_and_the_log(monkeypatch, stream):
    wire = Wire([reply(1, text="One."), reply(2, text="Two."), reply(3, text="Three.")]).install(
        monkeypatch, stream=stream
    )
    cfg = {"configurable": {"thread_id": THREAD, "user_id": "u-1"}}
    saver = InMemorySaver()
    with capture_logs() as logs:
        for text, system in (("Hi.", "Be brief."), ("More.", "Be brief."), ("Again.", "Be very brief.")):
            state = await _agent(saver, system).ainvoke({"messages": [HumanMessage(text)]}, cfg)

    dropped = [
        {"type": "thinking_dropped", "path": f"messages.{i}.content.0", "reason": "prefix_binding_mismatch"}
        for i in (1, 3)
    ]
    assert wire.transformations == [[], [], dropped]
    assert listed(state["messages"]) == [[], [], dropped]
    assert drops(state["messages"]) == {"prefix_binding_mismatch": 2}
    reports = [e for e in logs if e["event"] == "[ModelResilience] earlier thinking dropped by the provider"]
    assert [(r["thread_id"], r["prefix_binding_mismatch"], r["log_level"]) for r in reports] == [
        (THREAD, 2, "warning")
    ]
