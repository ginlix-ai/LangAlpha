"""langalpha's tools, offered to Claude Code through one in-process MCP server.

Claude never runs a langalpha tool itself. The CLI asks the server for a
call; the server waits for the runtime's ``tools`` step, which runs the
calls of the assistant message through the same ``ToolNode`` and
``wrap_tool_call`` middleware a langalpha turn uses, and hands each result
back. So a tool's ``wrap_tool_call`` policy (provenance, protected paths,
large-result eviction) and its state updates are the ones the turn already
has, and the ``tools`` step is the one place a call runs. Middleware that
runs around the model call (the credit gate, order approval) does not run
here; wiring the runtime into a turn has to place it.

A call is matched to its result by the tool-use id the CLI sends in the
request's ``_meta``, the id the assistant message carries.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from typing import Any

from claude_agent_sdk import McpSdkServerConfig
from langchain_anthropic.chat_models import convert_to_anthropic_tool
from langchain_core.messages import ToolMessage
from langchain_core.tools import BaseTool
from mcp.server import Server
from mcp.types import (
    BlobResourceContents,
    CallToolResult,
    EmbeddedResource,
    ImageContent,
    ListToolsResult,
    TextContent,
    Tool,
    ToolAnnotations,
)

from ptc_agent.agent.claude_code.relay import anthropic_content

SERVER = "langalpha"
TOOL_USE_ID_META = "claudecode/toolUseId"


def allow_rule(name: str) -> str:
    """The permission rule that lets Claude call one of our tools without asking.

    Claude sees our tools unprefixed (``CLAUDE_AGENT_SDK_MCP_NO_PREFIX``), but the
    CLI still matches an MCP tool's allow rule on its ``mcp__<server>__<name>``
    form: a bare ``Bash`` rule names the CLI's own Bash and leaves ours denied.
    """
    return f"mcp__{SERVER}__{name}"


class ToolResults:
    """Results the ``tools`` step produced, waiting for the CLI to ask for them."""

    def __init__(self) -> None:
        self._futures: dict[str, asyncio.Future[CallToolResult]] = {}
        #: Why the turn ended, once it has: a call that arrives after that fails at once.
        self._closed: str | None = None

    def _future(self, tool_use_id: str) -> asyncio.Future[CallToolResult]:
        fut = self._futures.get(tool_use_id)
        if fut is None:
            fut = asyncio.get_running_loop().create_future()
            self._futures[tool_use_id] = fut
            if self._closed is not None:
                fut.set_result(_failed(self._closed))
        return fut

    def resolve(self, message: ToolMessage) -> None:
        fut = self._future(message.tool_call_id)
        if not fut.done():
            fut.set_result(to_call_result(message))

    def fail_all(self, reason: str) -> None:
        self._closed = reason
        for fut in self._futures.values():
            if not fut.done():
                fut.set_result(_failed(reason))

    async def wait(self, tool_use_id: str) -> CallToolResult:
        return await self._future(tool_use_id)


def _failed(reason: str) -> CallToolResult:
    return CallToolResult(content=[TextContent(type="text", text=reason)], isError=True)


def to_call_result(message: ToolMessage) -> CallToolResult:
    """The result as MCP content, from the blocks a langalpha turn would send Claude.

    An image reaches Claude as an image. MCP has no document content, so a PDF
    goes as a resource, which the CLI saves and names to Claude in its own words.
    """
    content = anthropic_content(message)
    blocks: list[Any] = []
    for item in [{"type": "text", "text": content}] if isinstance(content, str) else content:
        source = item.get("source") or {}
        if item.get("type") == "text":
            blocks.append(TextContent(type="text", text=item.get("text", "")))
        elif item.get("type") == "image" and source.get("type") == "base64":
            blocks.append(ImageContent(type="image", data=source["data"], mimeType=source["media_type"]))
        elif item.get("type") == "document" and source.get("type") == "base64":
            blocks.append(EmbeddedResource(type="resource", resource=BlobResourceContents(
                uri=f"langalpha://{message.tool_call_id}/document", mimeType=source["media_type"], blob=source["data"])))
        else:
            blocks.append(TextContent(type="text", text=json.dumps(item, default=str)))
    return CallToolResult(content=blocks, isError=message.status == "error")


def tool_specs(tools: Sequence[BaseTool], read_only: frozenset[str] = frozenset()) -> list[Tool]:
    """MCP definitions with the schema and description a langalpha turn sends Claude."""
    specs = []
    for t in tools:
        wire = convert_to_anthropic_tool(t)
        specs.append(
            Tool(
                name=wire["name"],
                description=wire.get("description") or "",
                inputSchema=wire["input_schema"],
                annotations=ToolAnnotations(readOnlyHint=t.name in read_only),
            )
        )
    return specs


def build_tool_server(tools: Sequence[BaseTool], results: ToolResults, read_only: frozenset[str] = frozenset()) -> McpSdkServerConfig:
    specs = tool_specs(tools, read_only)
    names = {s.name for s in specs}

    async def on_list_tools(ctx: Any, params: Any) -> ListToolsResult:
        return ListToolsResult(tools=specs)

    async def on_call_tool(ctx: Any, params: Any) -> CallToolResult:
        meta = getattr(params, "meta", None) or {}
        if not isinstance(meta, dict):
            meta = meta.model_dump()
        tool_use_id = meta.get(TOOL_USE_ID_META)
        if params.name not in names or not tool_use_id:
            return CallToolResult(content=[TextContent(type="text", text=f"Unknown tool call {params.name}")], isError=True)
        return await results.wait(tool_use_id)

    server = Server(SERVER, version="1", on_list_tools=on_list_tools, on_call_tool=on_call_tool)
    return McpSdkServerConfig(type="sdk", name=SERVER, instance=server)
