from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from ptc_agent.agent.middleware.tool.asset_gate import AssetGateMiddleware
from ptc_agent.core.sandbox.asset_gate import AssetGate


def _request(name: str):
    return SimpleNamespace(tool_call={"name": name, "id": f"call-{name}", "args": {}})


@pytest.mark.asyncio
async def test_a_gated_call_runs_once_the_gate_opens_and_a_free_one_runs_while_it_is_held():
    gate = AssetGate()
    middleware = AssetGateMiddleware(gate, workspace_id="ws-a")
    middleware.free_of_computer(["web_search"])
    ran: list[str] = []

    async def handler(request):
        ran.append(request.tool_call["name"])
        return request.tool_call["name"]

    release = gate.hold("ws-a")
    gated = asyncio.create_task(middleware.awrap_tool_call(_request("bash"), handler))
    for _ in range(5):
        await asyncio.sleep(0)
    assert ran == []

    assert await middleware.awrap_tool_call(_request("web_search"), handler) == "web_search"
    assert ran == ["web_search"]
    assert not gated.done()

    release()
    assert await asyncio.wait_for(gated, 1) == "bash"
    assert ran == ["web_search", "bash"]
