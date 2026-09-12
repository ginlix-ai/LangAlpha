"""Gate for MCP tools bound directly to the model.

A directly bound tool is one JSON tool call, which is the shape a policy can
see. Two things run here that the sandbox path cannot offer: the consent the
connection carries is re-read on every call rather than trusted from the set
bound at turn start, and an order tool is stopped against a durable attempt
before the vendor sees it. ``direct_tool_middleware`` assembles both.

The policy itself is a port. This module ships in the agent library and knows
nothing about connection rows; the server hands it an object that does, the
same split ``CreditGateMiddleware`` keeps. Every call the binder stamped goes
through that port; a tool it did not stamp is not a direct tool and is left
alone.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ToolCallRequest
from langchain_core.messages import ToolMessage
from langchain_core.tools import BaseTool
from langgraph.errors import GraphBubbleUp
from langgraph.types import Command

from ptc_agent.agent.middleware.tool.error_handling import format_tool_error
from ptc_agent.core.mcp_sanitize import sanitize_tool_name

METADATA_KEY = "direct_mcp"


class DirectToolSet(Protocol):
    """What the server binds for one turn: the tools and the policy over them."""

    tools: list[BaseTool]

    async def check(self, server: str, tool: str, approval: bool = False) -> str | None:
        """None to allow the call, or the reason it is refused.

        ``approval`` is what the binder stamped on this tool at turn start, so
        the policy can tell a call that was gated from one that was not and
        refuse the latter if the setting has since changed under it.
        """
        ...


def direct_tool_meta(tool: Any) -> dict[str, Any] | None:
    """The binder's stamp on a tool, or None for any other tool."""
    meta = getattr(tool, "metadata", None) or {}
    stamp = meta.get(METADATA_KEY)
    return stamp if isinstance(stamp, dict) else None


def direct_tool_summary(tools: list[BaseTool]) -> str:
    """One prompt line per directly bound tool.

    A tool bound ``both`` keeps its sandbox wrapper, so its line says so: the
    blanket rule in the template is that these are called and not imported,
    and a ``both`` tool is the exception that the setting exists to buy.
    """
    lines: list[str] = []
    for tool in tools:
        summary = (tool.description or "").strip().split("\n", 1)[0]
        if len(summary) > 160:
            summary = summary[:157].rstrip() + "..."
        stamp = direct_tool_meta(tool) or {}
        line = f"- `{tool.name}`: {summary}"
        if stamp.get("approval"):
            line += " Asks the user to confirm before it runs."
        if stamp.get("sandboxed"):
            # The wrapper is generated under the sanitized name, so the raw
            # vendor name would send the model at a symbol its module does not
            # define. A name that cannot be salvaged has no symbol to point at
            # at all, so that tool simply loses the hint.
            func = sanitize_tool_name(str(stamp.get("tool") or ""))
            if func:
                line += (
                    f" (also importable as `tools.{stamp.get('server')}.{func}`"
                    " for batching in `execute_code`)"
                )
        lines.append(line)
    return "\n".join(lines)


def _stamped(result: ToolMessage | Command, stamp: dict[str, Any]) -> ToolMessage | Command:
    """Carry the vendor's own names out on the result.

    The tool name the model sees is derived from the pair and gives way to a
    digest when it cannot hold both, so it is not something a reader can parse
    back into a server and a tool. Riding on the message rather than on the
    streamed event means the identity survives a reload, which replays from the
    checkpoint and never sees the event.
    """
    if not isinstance(result, ToolMessage):
        return result
    artifact = result.artifact if isinstance(result.artifact, dict) else {}
    result.artifact = {**artifact, METADATA_KEY: dict(stamp)}
    return result


def refused_message(
    tool_call: Mapping[str, Any],
    reason: str,
    stamp: Mapping[str, Any],
    *,
    receipt: Mapping[str, Any] | None = None,
) -> ToolMessage:
    """A call the vendor never saw, shaped like the result it stands in for.

    ``reason`` is the whole text. A refusal is the one result whose identity
    cannot be recovered from the alias the model called, so it carries the
    stamp like any other; ``receipt`` is the ledger's own fragment, merged
    beside it and opaque here.
    """
    message = ToolMessage(
        content=reason,
        tool_call_id=tool_call["id"],
        name=tool_call.get("name"),
        status="error",
    )
    _stamped(message, dict(stamp))
    if receipt:
        artifact = message.artifact if isinstance(message.artifact, dict) else {}
        message.artifact = {**artifact, **receipt}
    return message


class DirectMcpPolicyMiddleware(AgentMiddleware):
    """Refuse a direct tool call the connection's current consent does not cover.

    Main-agent only, and placed before the approval interrupt so a refused
    call never asks the user to approve it. Any tool without the binder's
    stamp passes through untouched.
    """

    def __init__(self, toolset: DirectToolSet) -> None:
        self._toolset = toolset
        self._stamps: dict[str, dict[str, Any]] = {}
        for tool in toolset.tools:
            stamp = direct_tool_meta(tool)
            if stamp:
                self._stamps[tool.name] = stamp

    def _refused(
        self, request: ToolCallRequest, reason: str, stamp: dict[str, Any]
    ) -> ToolMessage:
        return refused_message(request.tool_call, f"Refused: {reason}", stamp)

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        stamp = self._stamps.get(request.tool_call.get("name", ""))
        if stamp is None:
            return handler(request)
        # The policy is async and so are the tools; a sync invocation of a
        # direct tool has no path to either, so it fails closed.
        return self._refused(
            request, "direct MCP tools run only on the async path", stamp
        )

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        stamp = self._stamps.get(request.tool_call.get("name", ""))
        if stamp is None:
            return await handler(request)
        try:
            # The policy check reads the connection row, so it fails the same
            # transient ways the call does and belongs under the same guard.
            reason = await self._toolset.check(
                stamp["server"], stamp["tool"], bool(stamp.get("approval"))
            )
            if reason:
                return self._refused(request, reason, stamp)
            result = await handler(request)
        except GraphBubbleUp:
            raise
        except Exception as e:
            # ``ToolErrorHandlingMiddleware`` converts a raising tool into an
            # error message, but it is installed ahead of this middleware and
            # the first wrapper is the outermost one, so it only sees the
            # exception after this frame has unwound and stamps nothing. A
            # relay timeout would then be the one result rendered without the
            # vendor's own names. Convert here instead, in the shape that
            # middleware would have used, and let it pass the message through.
            return _stamped(  # type: ignore[return-value]
                ToolMessage(
                    content=format_tool_error(e, request.tool_call.get("name")),
                    tool_call_id=request.tool_call["id"],
                    name=request.tool_call.get("name"),
                    status="error",
                ),
                stamp,
            )
        return _stamped(result, stamp)


def direct_tool_middleware(
    toolset: DirectToolSet | None, ledger: Any | None
) -> list[AgentMiddleware]:
    """The middleware a turn's directly bound tools need, outermost first.

    The order gate wraps the consent gate, so a consent refusal is an answer
    the gate records: the attempt it consumed settles as refused with a
    receipt, instead of staying ``approved`` with a call nothing ran. Consent
    still decides before any vendor is reached, because the inner wrapper runs
    before the tool does. Without a ledger no order runs at all, since the
    binder pins every order tool to this path.
    """
    # Local: ``order_governance`` imports this module for the result stamp.
    from ptc_agent.agent.middleware.order_governance import OrderGovernanceMiddleware

    tools = list(toolset.tools) if toolset is not None else []
    if not tools:
        return []
    stack: list[AgentMiddleware] = []
    if ledger is not None:
        stack.append(OrderGovernanceMiddleware(ledger, tools))
    stack.append(DirectMcpPolicyMiddleware(toolset))
    return stack
