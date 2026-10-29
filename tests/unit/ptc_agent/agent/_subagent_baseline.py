"""What a compiled subagent's runtime-context baseline sends on its first call."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage

from ptc_agent.agent.middleware.runtime_context import BaselineContextMiddleware


def baseline_of(compiled: dict[str, Any]) -> BaselineContextMiddleware | None:
    return next(
        (m for m in compiled.get("middleware", ()) if isinstance(m, BaselineContextMiddleware)),
        None,
    )


def baseline_text(compiled: dict[str, Any]) -> str:
    """The block the subagent's baseline appends, opened as a run would open it."""
    mw = baseline_of(compiled)
    assert mw is not None, "the subagent has no baseline"

    async def run() -> str:
        state: dict[str, Any] = {"messages": [HumanMessage(content="The brief.")]}
        state.update(await mw.abefore_agent(state) or {})
        captured: dict[str, Any] = {}

        async def handler(request: ModelRequest) -> ModelResponse:
            captured["system"] = request.system_message
            return MagicMock()

        await mw.awrap_model_call(
            ModelRequest(
                model=MagicMock(spec=ChatAnthropic),
                messages=state["messages"],
                system_prompt="Static system prompt.",
                state=state,
                tools=[],
            ),
            handler,
        )
        content = captured["system"].content
        return "\n".join(b["text"] for b in content[1:]) if isinstance(content, list) else ""

    return asyncio.run(run())
