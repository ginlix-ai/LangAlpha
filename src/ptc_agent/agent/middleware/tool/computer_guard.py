"""Refuse a machine tool's call that names a computer the tool cannot run on.

Only the Chief of Staff's own tools take ``computer``, and only while the user
has another computer. A call that names one anyway (a thread whose other
computer was removed, its history still calling it; a subagent told where the
work is) would have the argument dropped by the tool's schema and run here, on
a machine it did not mean, and its result would read as that computer's.
"""

from __future__ import annotations

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage

from ptc_agent.agent.tools.computer_reach import (
    COMPUTER_ARG,
    REACHABLE_TOOLS,
    ran_elsewhere,
)

_REFUSED = (
    "ERROR: This tool runs only on this computer and takes no `computer`, so "
    "the call did not run. Leave `computer` out to run it here."
)


class ComputerArgumentGuardMiddleware(AgentMiddleware):
    @staticmethod
    def _refusal(request) -> ToolMessage | None:
        call = request.tool_call
        tool = request.tool
        if (
            call.get("name") in REACHABLE_TOOLS
            and ran_elsewhere(call.get("args"))
            and tool is not None
            and COMPUTER_ARG not in tool.args
        ):
            return ToolMessage(
                content=_REFUSED,
                tool_call_id=call["id"],
                name=call.get("name"),
                status="error",
            )
        return None

    def wrap_tool_call(self, request, handler):
        return self._refusal(request) or handler(request)

    async def awrap_tool_call(self, request, handler):
        return self._refusal(request) or await handler(request)
