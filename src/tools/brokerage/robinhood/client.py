"""
Async MCP client for the Robinhood remote trading MCP server.

Opens a fresh session per call — each call injects a per-user Bearer token
into a short-lived httpx.AsyncClient passed to the MCP transport.
"""
from __future__ import annotations

import json
import logging
from typing import Any

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.types import CallToolResult, TextContent

ROBINHOOD_MCP_URL = "https://agent.robinhood.com/mcp/trading"

logger = logging.getLogger(__name__)


class RobinhoodMCPError(Exception):
    """Raised when the Robinhood MCP server returns a tool error."""


async def call_tool(token: str, tool_name: str, arguments: dict[str, Any]) -> Any:
    """
    Call a Robinhood MCP tool with the given Bearer token.

    Returns parsed JSON when the response is valid JSON, otherwise returns the
    raw text string. Raises RobinhoodMCPError when the server signals isError.
    """
    async with httpx.AsyncClient(
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0,
    ) as http_client:
        async with streamable_http_client(
            url=ROBINHOOD_MCP_URL,
            http_client=http_client,
        ) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result: CallToolResult = await session.call_tool(tool_name, arguments)

    if result.isError:
        error_text = " ".join(
            block.text for block in result.content if isinstance(block, TextContent)
        )
        raise RobinhoodMCPError(error_text or f"Robinhood MCP: {tool_name} returned an error")

    text = "".join(block.text for block in result.content if isinstance(block, TextContent))
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return text
