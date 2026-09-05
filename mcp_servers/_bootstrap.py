"""Runtime shims for MCP server subprocesses: ``sys.path`` and the SDK server class.

Shared code imports as both ``src.*`` (the backend's spelling) and bare
``data_client.*``, but a launched server only gets ``/app/src`` on the path
(uv editable install). Importing this module first — its launcher directory is
``sys.path[0]`` — inserts the repo root so both spellings resolve.

``MCPServer`` resolves to the installed SDK's server class so the same server
files run on mcp 2.x images and on warm sandboxes still carrying mcp 1.x.
"""

import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

try:
    from mcp.server.mcpserver import MCPServer
except ImportError:  # mcp 1.x sandbox image
    from mcp.server.fastmcp import FastMCP as MCPServer

__all__ = ["MCPServer"]
