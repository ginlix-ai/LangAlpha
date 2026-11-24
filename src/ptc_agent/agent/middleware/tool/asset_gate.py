"""Hold a tool call until its computer's synced files have settled.

A turn starts while the host is still bringing the computer's tool modules,
packages and skills up to date (``core/sandbox/asset_gate.py``). The model's
first response needs none of them; a call that runs on the computer does. So
every tool call waits here for the gate, except the tools named free of the
computer, which reach only the server. A tool nobody named waits: a wait it
did not need costs the length of a sync once, where running ahead of the sync
would run code against half-replaced files.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable

import structlog
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ToolCallRequest
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from ptc_agent.core.sandbox.asset_gate import AssetGate

logger = structlog.get_logger(__name__)


class AssetGateMiddleware(AgentMiddleware):
    """Wait for the computer's gate ahead of each call that may run on it.

    Sits inside the middleware that refuses a call (a repeat, a protected
    path, invalid code), so a refused call answers at once, and outside the
    ones that reach the computer for a call they let through.
    """

    def __init__(self, gate: AssetGate, *, workspace_id: str | None) -> None:
        super().__init__()
        self._gate = gate
        self._workspace_id = workspace_id
        self._free: set[str] = set()

    def free_of_computer(self, names: Iterable[str]) -> None:
        """Name tools that never reach the computer, which skip the wait.
        Filled as the agent's tools are assembled, before any call runs."""
        self._free.update(names)

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        name = request.tool_call.get("name") or ""
        if name not in self._free:
            waited = await self._gate.wait(self._workspace_id)
            if waited:
                logger.info(
                    f"[ASSET_WAIT] tool={name} waited={waited * 1000:.0f}ms "
                    f"settled={self._gate.settled(self._workspace_id)}"
                )
        return await handler(request)
