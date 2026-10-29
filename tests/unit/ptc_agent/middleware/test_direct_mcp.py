"""The direct-tool gate: refuses on the port's word, passes everything else."""

from __future__ import annotations

import pytest
from langchain_core.messages import ToolMessage
from langchain_core.tools import StructuredTool

from langgraph.errors import GraphBubbleUp

from ptc_agent.agent.middleware.direct_mcp import (
    METADATA_KEY,
    DirectMcpPolicyMiddleware,
    direct_tool_middleware,
    direct_tool_summary,
    refused_message,
)
from ptc_agent.agent.middleware.tool.error_handling import format_tool_error


def _tool(name: str, *, stamp: dict | None) -> StructuredTool:
    tool = StructuredTool.from_function(func=lambda: "ok", name=name, description="t")
    if stamp is not None:
        tool.metadata = {METADATA_KEY: stamp}
    return tool


class _Request:
    def __init__(self, name: str) -> None:
        self.tool_call = {"id": "call-1", "name": name, "args": {}}


class _ToolSet:
    def __init__(self, tools, reason=None):
        self.tools = tools
        self.reason = reason
        self.asked: list[tuple[str, str, bool]] = []

    async def check(self, server, tool, approval=False):
        self.asked.append((server, tool, approval))
        return self.reason


async def _ran(request):
    return ToolMessage(content="ran", tool_call_id=request.tool_call["id"])


DIRECT = _tool(
    "mcp__moomoo__sim_trade_input_order",
    stamp={"server": "moomoo", "tool": "sim_trade_input_order"},
)

# Approval only ever rides on an order tool: nothing else is gated.
GATED = _tool(
    "mcp__moomoo__trading_order_place",
    stamp={
        "server": "moomoo",
        "tool": "trading_order_place",
        "order": {"action": "place", "mode": "live"},
        "approval": True,
    },
)


@pytest.mark.asyncio
async def test_unstamped_tool_passes_without_consulting_the_port():
    toolset = _ToolSet([DIRECT], reason="never")
    mw = DirectMcpPolicyMiddleware(toolset)
    out = await mw.awrap_tool_call(_Request("web_search"), _ran)
    assert out.content == "ran"
    assert toolset.asked == []


@pytest.mark.asyncio
async def test_direct_tool_runs_when_the_port_allows_it():
    toolset = _ToolSet([DIRECT])
    mw = DirectMcpPolicyMiddleware(toolset)
    out = await mw.awrap_tool_call(_Request(DIRECT.name), _ran)
    assert out.content == "ran"
    assert toolset.asked == [("moomoo", "sim_trade_input_order", False)]


@pytest.mark.asyncio
async def test_direct_tool_is_refused_with_the_ports_reason():
    mw = DirectMcpPolicyMiddleware(_ToolSet([DIRECT], reason="consent withdrawn"))
    out = await mw.awrap_tool_call(_Request(DIRECT.name), _ran)
    assert isinstance(out, ToolMessage)
    assert out.status == "error"
    assert out.tool_call_id == "call-1"
    assert out.content == "Refused: consent withdrawn"
    # The refusal is the one result whose identity the surface cannot recover
    # from the alias, because it is also the one the vendor never answered.
    assert out.artifact["direct_mcp"] == {
        "server": "moomoo",
        "tool": "sim_trade_input_order",
    }


def test_sync_path_fails_closed_for_direct_tools_only():
    mw = DirectMcpPolicyMiddleware(_ToolSet([DIRECT]))
    ran = mw.wrap_tool_call(
        _Request("web_search"),
        lambda r: ToolMessage(content="ran", tool_call_id="call-1"),
    )
    assert ran.content == "ran"
    refused = mw.wrap_tool_call(
        _Request(DIRECT.name),
        lambda r: ToolMessage(content="ran", tool_call_id="call-1"),
    )
    assert refused.status == "error"
    assert refused.artifact["direct_mcp"]["server"] == "moomoo"


@pytest.mark.asyncio
async def test_a_permitted_call_carries_the_vendor_names_out():
    """The alias is lossy, so the result is where identity has to travel."""
    tool = _tool(
        "mcp__moomoo__sim_trade_input_order",
        stamp={"server": "moomoo", "tool": "sim_trade_input_order"},
    )
    mw = DirectMcpPolicyMiddleware(_ToolSet([tool], reason=None))

    async def handler(request):
        return ToolMessage(content="ok", tool_call_id="1")

    out = await mw.awrap_tool_call(_Request(tool.name), handler)
    assert out.artifact["direct_mcp"] == {
        "server": "moomoo",
        "tool": "sim_trade_input_order",
    }


@pytest.mark.asyncio
async def test_the_stamp_does_not_displace_an_artifact_the_tool_set():
    tool = _tool("mcp__moomoo__quote", stamp={"server": "moomoo", "tool": "quote"})
    mw = DirectMcpPolicyMiddleware(_ToolSet([tool], reason=None))

    async def handler(request):
        return ToolMessage(content="ok", tool_call_id="1", artifact={"rows": 3})

    out = await mw.awrap_tool_call(_Request(tool.name), handler)
    assert out.artifact["rows"] == 3
    assert out.artifact["direct_mcp"]["tool"] == "quote"


@pytest.mark.asyncio
async def test_a_raising_vendor_call_still_carries_the_vendor_names_out():
    # ToolErrorHandlingMiddleware is installed ahead of this one and the first
    # wrapper is the outermost, so it converts the exception only after this
    # frame unwound. Without converting here, a relay timeout is the one
    # result the surface renders under the alias instead of the vendor.
    mw = DirectMcpPolicyMiddleware(_ToolSet([DIRECT]))

    async def boom(request):
        raise TimeoutError("relay wall clock exceeded")

    out = await mw.awrap_tool_call(_Request(DIRECT.name), boom)

    assert isinstance(out, ToolMessage)
    assert out.status == "error"
    assert out.tool_call_id == "call-1"
    assert out.artifact["direct_mcp"] == {
        "server": "moomoo",
        "tool": "sim_trade_input_order",
    }
    # Reported in the shape the converter would have used, so the two error
    # paths cannot drift into two different sentences for one failure.
    assert out.content == format_tool_error(
        TimeoutError("relay wall clock exceeded"), DIRECT.name
    )


@pytest.mark.asyncio
async def test_a_graph_interrupt_is_not_swallowed_as_a_tool_error():
    # The converter re-raises GraphBubbleUp because LangGraph control flow
    # travels as an exception; catching it here would strand the interrupt.
    mw = DirectMcpPolicyMiddleware(_ToolSet([DIRECT]))

    async def interrupt(request):
        raise GraphBubbleUp("resume me")

    with pytest.raises(GraphBubbleUp):
        await mw.awrap_tool_call(_Request(DIRECT.name), interrupt)


@pytest.mark.asyncio
async def test_a_policy_check_that_raises_is_stamped_like_the_call():
    # The check reads the connection row, so it fails the same transient ways
    # the vendor call does, and it sits before the handler: left outside the
    # guard it would be the one remaining path that unwinds without identity.
    class Broken(_ToolSet):
        async def check(self, server, tool, approval=False):
            raise ConnectionError("connection row unavailable")

    mw = DirectMcpPolicyMiddleware(Broken([DIRECT]))

    out = await mw.awrap_tool_call(_Request(DIRECT.name), _ran)

    assert out.status == "error"
    assert out.artifact["direct_mcp"]["tool"] == "sim_trade_input_order"
    assert "connection row unavailable" in out.content


class TestDirectToolSummaryImportHint:
    """A `both` tool's line has to name the symbol the wrapper actually defines."""

    def test_a_direct_only_tool_is_not_offered_an_import(self):
        line = direct_tool_summary(
            [
                _tool(
                    "mcp__moomoo__place",
                    stamp={
                        "server": "moomoo",
                        "tool": "trading_order_place",
                        "sandboxed": False,
                    },
                )
            ]
        )
        assert "importable" not in line

    def test_the_hint_uses_the_generated_name_not_the_vendor_name(self):
        # The wrapper is emitted under _safe_func_name, so a vendor name with
        # illegal characters is a different symbol in the module.
        line = direct_tool_summary(
            [
                _tool(
                    "mcp__acme__odd",
                    stamp={"server": "acme", "tool": "weird-name.v2", "sandboxed": True},
                )
            ]
        )
        assert "`tools.acme.weird_name_v2`" in line
        assert "weird-name.v2`" not in line

    def test_a_name_with_no_legal_form_loses_the_hint(self):
        line = direct_tool_summary(
            [_tool("mcp__acme__bad", stamp={"server": "acme", "tool": "---", "sandboxed": True})]
        )
        assert "importable" not in line


class TestDirectToolMiddleware:
    """What a turn installs for its directly bound tools, and in what order."""

    def test_the_order_gate_wraps_the_consent_gate(self):
        # The first wrapper is the outermost, so a consent refusal comes back
        # through the order gate and settles the attempt it consumed.
        stack = direct_tool_middleware(_ToolSet([GATED]), object())
        assert [type(m).__name__ for m in stack] == [
            "OrderGovernanceMiddleware",
            "DirectMcpPolicyMiddleware",
        ]

    def test_without_a_ledger_nothing_governs_an_order(self):
        stack = direct_tool_middleware(_ToolSet([GATED]), None)
        assert [type(m).__name__ for m in stack] == ["DirectMcpPolicyMiddleware"]

    def test_a_turn_with_no_direct_tools_installs_nothing(self):
        assert direct_tool_middleware(_ToolSet([]), object()) == []
        assert direct_tool_middleware(None, object()) == []


@pytest.mark.asyncio
async def test_the_gate_stamp_is_what_the_port_is_asked_about():
    """The port refuses a call bound ungated on a row that now gates it, so it
    has to be told what this turn stamped rather than re-deriving it."""
    toolset = _ToolSet([GATED])
    mw = DirectMcpPolicyMiddleware(toolset)
    out = await mw.awrap_tool_call(_Request(GATED.name), _ran)
    assert out.content == "ran"
    assert toolset.asked == [("moomoo", "trading_order_place", True)]


class TestRefusedMessage:
    """The one builder both gates refuse through."""

    def test_the_caller_owns_the_whole_sentence(self):
        # The order gate names the order in its own words, so a prefix added
        # here would arrive in the middle of somebody else's sentence.
        out = refused_message(
            {"id": "call-1", "name": "mcp__moomoo__place"},
            "an order (live place at moomoo) runs only on the async path",
            {"server": "moomoo", "tool": "trading_order_place"},
        )
        assert out.status == "error"
        assert out.tool_call_id == "call-1"
        assert out.content.startswith("an order")
        assert out.artifact[METADATA_KEY] == {
            "server": "moomoo",
            "tool": "trading_order_place",
        }

    def test_a_receipt_rides_beside_the_stamp(self):
        out = refused_message(
            {"id": "call-1", "name": "mcp__moomoo__place"},
            "refused",
            {"server": "moomoo", "tool": "trading_order_place"},
            receipt={"order_attempt": {"id": "900001"}},
        )
        assert out.artifact["order_attempt"] == {"id": "900001"}
        assert out.artifact[METADATA_KEY]["server"] == "moomoo"
