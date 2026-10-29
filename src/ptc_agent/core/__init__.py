"""Open PTC Agent - PTC Core Infrastructure.

An open source implementation of Programmatic Tool Calling (PTC) with MCP,
where agents generate executable Python code to interact with MCP servers.

This package provides the core infrastructure:
- PTCSandbox: Daytona sandbox management
- MCPRegistry: MCP server connections and tool discovery
- ToolFunctionGenerator: Convert MCP schemas to Python functions
- Session/SessionManager: Session lifecycle management
- Config: Configuration loading

For agent implementations, see the agent package.
"""

__version__ = "0.1.0"

from ptc_agent.config.core import CoreConfig

from .mcp_registry import MCPRegistry, MCPToolInfo
from .sandbox import PTCSandbox
from .session import Session, SessionManager
from .tool_generator import ToolFunctionGenerator

__all__ = [
    "CoreConfig",
    "MCPRegistry",
    "MCPToolInfo",
    "PTCSandbox",
    "Session",
    "SessionManager",
    "ToolFunctionGenerator",
]
