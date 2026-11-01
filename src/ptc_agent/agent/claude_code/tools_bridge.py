"""langalpha's tools, offered to Claude Code through one in-process MCP server.

Claude never runs a langalpha tool itself. The CLI asks the server for a
call; the server waits for the runtime's ``tools`` step, which runs each
call the CLI asks for, with the arguments the CLI sends (a hook may have
rewritten them), through the same ``ToolNode`` and ``wrap_tool_call``
middleware a langalpha turn uses, and hands each result back. A call the
CLI refuses itself (its permission rules, a hook, a tool it does not have)
never reaches the server, and the step records the CLI's own answer instead. So a tool's ``wrap_tool_call`` policy (provenance, protected paths,
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
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any

from claude_agent_sdk import McpSdkServerConfig, ToolResultBlock
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
from ptc_agent.agent.claude_code.session import ToolDefinition

SERVER = "langalpha"
TOOL_USE_ID_META = "claudecode/toolUseId"
#: The most of one tool result the CLI hands Claude whole. Past it the CLI
#: saves the result to a file Claude has no tool to read and sends a 2 KB
#: preview instead; its own limit is 50,000 characters, and 500,000 is the
#: most it accepts. At that, our own eviction (about 160,000 characters by
#: default) is what cuts a large result, as on any other turn.
MAX_RESULT_CHARS = 500_000


def allow_rule(name: str) -> str:
    """The permission rule that lets Claude call one of our tools without asking.

    Claude sees our tools unprefixed (``CLAUDE_AGENT_SDK_MCP_NO_PREFIX``), but the
    CLI still matches an MCP tool's allow rule on its ``mcp__<server>__<name>``
    form: a bare ``Bash`` rule names the CLI's own Bash and leaves ours denied.
    """
    return f"mcp__{SERVER}__{name}"


@dataclass(frozen=True)
class Run:
    """The CLI asked us to run the call, with these arguments (a hook may have rewritten them)."""

    args: dict[str, Any]


@dataclass(frozen=True)
class Answered:
    """The CLI gave Claude its own answer without asking: its refusal."""

    block: ToolResultBlock


@dataclass(frozen=True)
class Ended:
    """The CLI's query ended before it asked for the call or answered it."""


#: What the CLI made of a call, whichever came first.
Due = Run | Answered | Ended


@dataclass
class _Call:
    due: asyncio.Future[Due]
    #: The result the ``tools`` step produced for the call, which the CLI waits on.
    result: asyncio.Future[CallToolResult]
    #: The result the CLI gave Claude for the call.
    given: ToolResultBlock | None = None
    #: The step's result as Anthropic blocks, where it holds one MCP cannot
    #: carry (a PDF), for the PostToolUse hook to hand the CLI instead.
    blocks: list[dict[str, Any]] | None = None


class ToolResults:
    """Each call between the CLI and the ``tools`` step.

    The query's end and the run's close are two moments. Once the query
    ends, a call the CLI has not asked for or answered never will be, while
    a call the step is running still delivers its result. Once the run
    closes, a result still owed fails with the reason.
    """

    def __init__(self) -> None:
        self._calls: dict[str, _Call] = {}
        self._query_ended = False
        self._closed: str | None = None

    def _call(self, tool_use_id: str) -> _Call:
        call = self._calls.get(tool_use_id)
        if call is None:
            loop = asyncio.get_running_loop()
            call = self._calls[tool_use_id] = _Call(loop.create_future(), loop.create_future())
            if self._query_ended:
                call.due.set_result(Ended())
            if self._closed is not None:
                call.result.set_result(_failed(self._closed))
        return call

    def resolve(self, message: ToolMessage) -> None:
        call = self._call(message.tool_call_id)
        if call.result.done():
            return
        content = anthropic_content(message)
        if isinstance(content, list) and any(b.get("type") == "document" for b in content):
            call.blocks = content
        call.result.set_result(to_call_result(message))

    def blocks(self, tool_use_id: str) -> list[dict[str, Any]] | None:
        """The result to hand the CLI in place of its MCP form, where that form lost a block."""
        call = self._calls.get(tool_use_id)
        return call.blocks if call is not None else None

    def refused(self, tool_use_id: str) -> bool:
        """Whether the CLI answered the call itself without asking for it."""
        call = self._calls.get(tool_use_id)
        return call is not None and call.due.done() and isinstance(call.due.result(), Answered)

    def fail_all(self, reason: str) -> None:
        self._closed = reason
        for call in self._calls.values():
            if not call.result.done():
                call.result.set_result(_failed(reason))

    def due(self, tool_use_id: str) -> asyncio.Future[Due]:
        return self._call(tool_use_id).due

    def _settle(self, tool_use_id: str, due: Due) -> None:
        future = self.due(tool_use_id)
        if not future.done():
            future.set_result(due)

    async def wait(self, tool_use_id: str, arguments: dict[str, Any]) -> CallToolResult:
        self._settle(tool_use_id, Run(arguments))
        return await self._call(tool_use_id).result

    def answered(self, blocks: Sequence[ToolResultBlock]) -> None:
        for b in blocks:
            self._call(b.tool_use_id).given = b
            self._settle(b.tool_use_id, Answered(b))

    def ended(self) -> None:
        self._query_ended = True
        for call in self._calls.values():
            if not call.due.done():
                call.due.set_result(Ended())

    def reopen(self) -> None:
        """The turn sent the CLI another query: its calls are new ones."""
        self._query_ended = False

    def given_instead(self, tool_use_id: str, name: str) -> ToolResultBlock | None:
        """The CLI's answer to a call it ran through us, where Claude was given
        other text than the step's result: a PostToolUse hook's replacement,
        or the CLI's preview of a result past its size limit.

        The CLI hands our text over as it is, keeps an image (re-encoded when
        large, so only its place is compared) with a note of where it saved
        it, puts a resource's note in its place, and words an empty result
        itself; none of that is another answer.
        """
        call = self._calls.get(tool_use_id)
        if call is None or call.given is None or not call.result.done():
            return None
        block, ours = call.given, call.result.result().content
        given = [{"type": "text", "text": block.content}] if isinstance(block.content, str) else list(block.content or [])
        if call.blocks is not None and given == call.blocks:
            return None
        if ours == [TextContent(type="text", text="")] and given == [{"type": "text", "text": f"({name} completed with no output)"}]:
            return None
        expected: list[Callable[[dict[str, Any]], bool]] = []
        for c in ours:
            if isinstance(c, TextContent):
                expected.append(partial(_is_text, c.text))
            elif isinstance(c, ImageContent):
                expected += [_is_image, _is_image_note]
            else:
                expected.append(partial(_is_resource_note, str(c.resource.uri)))
        return None if len(given) == len(expected) and all(is_ok(part) for is_ok, part in zip(expected, given, strict=True)) else block


#: The notes the CLI puts in a result Claude is given for each image it
#: saved, and in place of each resource it saved. Our own text is never
#: read as a note, however it reads.
_IMAGE_NOTE = re.compile(r"\[Image: source: [^\]\n]+\]")
_RESOURCE_NOTE = re.compile(r"\[Resource from langalpha at (\S+)\] Binary content \([^)\n]*\) saved to [^\n]+")


def _is_text(text: str, part: dict[str, Any]) -> bool:
    return part.get("type") == "text" and part.get("text", "") == text


def _is_image(part: dict[str, Any]) -> bool:
    return part.get("type") == "image"


def _is_image_note(part: dict[str, Any]) -> bool:
    return part.get("type") == "text" and _IMAGE_NOTE.fullmatch(part.get("text", "")) is not None


def _is_resource_note(uri: str, part: dict[str, Any]) -> bool:
    found = _RESOURCE_NOTE.fullmatch(part.get("text", "")) if part.get("type") == "text" else None
    return found is not None and found.group(1) == uri


def _failed(reason: str) -> CallToolResult:
    return CallToolResult(content=[TextContent(type="text", text=reason)], isError=True)


def to_call_result(message: ToolMessage) -> CallToolResult:
    """The result as MCP content, from the blocks a langalpha turn would send Claude.

    An image reaches Claude as an image. MCP has no document content, so a PDF
    goes as a resource, which the CLI would save and name to Claude in its own
    words; the runtime's PostToolUse hook hands the CLI the document block in
    its place (``ToolResults.blocks``).
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


def tool_specs(tools: Sequence[BaseTool], read_only: frozenset[str] = frozenset()) -> list[ToolDefinition]:
    """The definitions with the schema and description a langalpha turn sends
    Claude, as plain data a session can keep (``session.SessionPin``)."""
    specs: list[ToolDefinition] = []
    for t in tools:
        wire = convert_to_anthropic_tool(t)
        specs.append({
            "name": wire["name"],
            "description": wire.get("description") or "",
            "input_schema": wire["input_schema"],
            "read_only": t.name in read_only,
        })
    return specs


def build_tool_server(definitions: Sequence[ToolDefinition], results: ToolResults) -> McpSdkServerConfig:
    """The server offering ``definitions``. A call to one the current build no
    longer has still reaches the ``tools`` step, whose tool node refuses it.
    One this server answers itself never reaches the step, which records
    the answer as the CLI passes it on."""
    specs = [
        Tool(
            name=d["name"],
            description=d["description"],
            inputSchema=d["input_schema"],
            annotations=ToolAnnotations(readOnlyHint=d["read_only"]),
            _meta={"anthropic/maxResultSizeChars": MAX_RESULT_CHARS},
        )
        for d in definitions
    ]
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
        return await results.wait(tool_use_id, params.arguments or {})

    server = Server(SERVER, version="1", on_list_tools=on_list_tools, on_call_tool=on_call_tool)
    return McpSdkServerConfig(type="sdk", name=SERVER, instance=server)
