"""MCP Server Registry - Connect to and manage external MCP servers."""

import asyncio
import os
from types import TracebackType
from typing import Any

import httpx
import structlog
from mcp import ClientSession, StdioServerParameters
from mcp.client.sse import sse_client
from mcp.client.stdio import stdio_client

from ptc_agent.config.core import CoreConfig, MCPServerConfig

logger = structlog.get_logger(__name__)

# Discard MCP subprocess stderr — noisy INFO logs (e.g. "Processing request
# of type ListToolsRequest") and failures surface as connection errors in
# our process instead.  Needs a real FD because the MCP SDK passes it as
# stderr to subprocess.Popen.
_devnull = open(os.devnull, "w")  # noqa: SIM115


class MCPToolInfo:
    """Snapshot of a single tool's schema as reported by its MCP server."""

    def __init__(
        self,
        name: str,
        description: str,
        input_schema: dict[str, Any],
        server_name: str,
    ) -> None:
        self.name = name
        self.description = description
        self.input_schema = input_schema
        self.server_name = server_name

    def get_parameters(self) -> dict[str, Any]:
        """Return ``{param_name: {type, description, required, default}}`` from input_schema."""
        params = {}

        if "properties" in self.input_schema:
            required_params = self.input_schema.get("required", [])

            for param_name, param_info in self.input_schema["properties"].items():
                params[param_name] = {
                    "type": param_info.get("type", "any"),
                    "description": param_info.get("description", ""),
                    "required": param_name in required_params,
                    "default": param_info.get("default"),
                }

        return params

    def _extract_return_type_from_description(self) -> str:
        """Extract return type hint from description's Returns: section.

        Returns:
            Type hint string (e.g., "dict", "list[dict]") or "Any" if not found
        """
        import re

        if not self.description:
            return "Any"

        # Look for common type indicators after "Returns:"
        match = re.search(
            r"Returns?:\s*\n?\s*(\w+(?:\[[\w,\s]+\])?)",
            self.description,
            re.IGNORECASE
        )

        if match:
            type_str = match.group(1).lower()
            type_map = {
                "dict": "dict",
                "dictionary": "dict",
                "list": "list",
                "array": "list",
                "str": "str",
                "string": "str",
                "int": "int",
                "integer": "int",
                "float": "float",
                "number": "float",
                "bool": "bool",
                "boolean": "bool",
            }
            return type_map.get(type_str, "Any")

        return "Any"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return_type = self._extract_return_type_from_description()
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.get_parameters(),
            "server_name": self.server_name,
            "return_type": return_type,
        }


class MCPServerConnector:
    """Connector for an individual MCP server.

    Uses nested async with pattern following MCP SDK best practices.
    The connector acts as an async context manager that keeps the
    stdio_client and ClientSession contexts alive.
    """

    def __init__(self, config: MCPServerConfig) -> None:
        self.config = config
        self.session: ClientSession | None = None
        self.tools: list[MCPToolInfo] = []

        # Background task management
        self._connection_task: asyncio.Task | None = None
        self._ready: asyncio.Event = asyncio.Event()
        self._disconnect_event: asyncio.Event = asyncio.Event()
        self._connection_error: Exception | None = None

        logger.debug("Initialized MCPServerConnector", server=config.name)

    # Env vars safe to forward to MCP server subprocesses.
    # Prevents leaking host secrets (ANTHROPIC_API_KEY, DB_PASSWORD, etc.)
    # to MCP discovery processes. Servers that need additional env vars
    # must declare them explicitly in their config's `env:` block.
    _SAFE_ENV_VARS = frozenset({
        # OS basics
        "PATH", "HOME", "USER", "LOGNAME", "SHELL", "TERM", "LANG",
        "LC_ALL", "LC_CTYPE", "LC_MESSAGES",
        # Temp dirs
        "TMPDIR", "TMP", "TEMP",
        # Node.js (MCP servers are often npx packages)
        "NODE_PATH", "NPM_CONFIG_PREFIX", "NODE_OPTIONS", "NODE_ENV",
        # Python (for uv/pip-based MCP servers)
        "PYTHONPATH", "VIRTUAL_ENV",
        # XDG
        "XDG_RUNTIME_DIR", "XDG_DATA_HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME",
    })

    def _prepare_env(self) -> dict[str, str]:
        """Prepare environment variables by expanding placeholders.

        Starts from a safe subset of os.environ (not the full environment)
        to prevent leaking host secrets to MCP server subprocesses.
        Servers that need specific env vars must declare them in their
        config's `env:` block.

        Returns:
            Dictionary with safe base vars + expanded declared env vars
        """
        base_env = {k: os.environ[k] for k in self._SAFE_ENV_VARS if k in os.environ}

        if not self.config.env:
            return base_env

        for key, value in self.config.env.items():
            if isinstance(value, str):
                expanded_value = os.path.expandvars(value)
                base_env[key] = expanded_value

                if "${" in value and expanded_value != value:
                    logger.debug(
                        "Expanded environment variable",
                        server=self.config.name,
                        var=key,
                        from_placeholder=value,
                    )
            else:
                base_env[key] = value

        return base_env

    def _expand_url(self) -> str | None:
        """Return the URL with ``${VAR}`` placeholders expanded, or None if unconfigured."""
        if not self.config.url:
            return None

        expanded_url = os.path.expandvars(self.config.url)

        if "${" in self.config.url and expanded_url != self.config.url:
            logger.debug(
                "Expanded URL environment variables",
                server=self.config.name,
            )

        # Warn if expansion failed (env var not set)
        if "${" in expanded_url:
            logger.warning(
                "URL contains unexpanded environment variables - check if env var is set",
                server=self.config.name,
                url=self.config.url,
            )

        return expanded_url

    async def __aenter__(self) -> "MCPServerConnector":
        """Start the background connection task and wait for it to be ready."""
        logger.debug("Connecting to MCP server", server=self.config.name)

        # Start background task that keeps nested contexts alive
        self._connection_task = asyncio.create_task(
            self._run_connection(), name=f"mcp-{self.config.name}"
        )

        # Wait for connection to be ready or fail
        await self._ready.wait()

        if self._connection_error:
            raise self._connection_error

        logger.debug(
            "Connected to MCP server",
            server=self.config.name,
            tool_count=len(self.tools),
        )

        return self

    async def _run_connection(self) -> None:
        """Background task that maintains the nested async with contexts.

        This follows MCP SDK best practices by using proper nested async with
        statements within a single task, ensuring contexts are entered and
        exited in LIFO order within the same task.
        """
        try:
            if self.config.transport == "http":
                # HTTP transport - use direct JSON-RPC over HTTP POST
                url = self._expand_url()
                if not url:
                    msg = f"URL required for HTTP transport: {self.config.name}"
                    raise ValueError(msg)

                # HTTP transport doesn't use ClientSession - we make direct requests
                self._http_url = url
                self._http_client = httpx.AsyncClient(timeout=60.0)
                self._message_id = 0

                # Discover tools via HTTP
                await self._discover_tools_http()

                logger.debug(
                    "MCP HTTP connection established",
                    server=self.config.name,
                )

                # Signal that connection is ready
                self._ready.set()

                # Keep alive until disconnect
                await self._disconnect_event.wait()

                # Cleanup
                await self._http_client.aclose()

                logger.debug(
                    "MCP HTTP connection disconnect signaled",
                    server=self.config.name,
                )

            elif self.config.transport == "sse":
                # SSE transport - use URL-based connection
                url = self._expand_url()
                if not url:
                    msg = f"URL required for SSE transport: {self.config.name}"
                    raise ValueError(msg)

                async with sse_client(url) as (read_stream, write_stream), ClientSession(read_stream, write_stream) as session:
                    self.session = session

                    # Initialize and discover tools
                    # SSE connections need retry due to endpoint event timing
                    await self.session.initialize()
                    await self._discover_tools_with_retry()

                    logger.debug(
                        "MCP SSE connection established",
                        server=self.config.name,
                    )

                    # Signal that connection is ready
                    self._ready.set()

                    # Keep contexts alive until disconnect is signaled
                    await self._disconnect_event.wait()

                    logger.debug(
                        "MCP SSE connection disconnect signaled",
                        server=self.config.name,
                    )
            else:
                # Stdio transport (default) - use command-based connection
                if not self.config.command:
                    raise ValueError("Command is required for stdio transport")
                server_params = StdioServerParameters(
                    command=self.config.command,
                    args=self.config.args,
                    env=self._prepare_env(),
                )

                # Proper nested async with pattern (MCP SDK best practice)
                async with stdio_client(server_params, errlog=_devnull) as (read_stream, write_stream):
                    async with ClientSession(read_stream, write_stream) as session:
                        self.session = session

                        # Initialize and discover tools
                        await self.session.initialize()
                        await self._discover_tools()

                        logger.debug(
                            "MCP connection contexts established",
                            server=self.config.name,
                        )

                        # Signal that connection is ready
                        self._ready.set()

                        # Keep contexts alive until disconnect is signaled
                        await self._disconnect_event.wait()

                        logger.debug(
                            "MCP connection disconnect signaled",
                            server=self.config.name,
                        )

        except Exception as e:
            # Store error and signal ready so __aenter__ can raise it
            self._connection_error = e
            self._ready.set()

            import traceback
            error_details = traceback.format_exc()

            logger.error(
                "Failed to connect to MCP server",
                server=self.config.name,
                error=str(e),
                error_type=type(e).__name__,
                traceback=error_details,
            )

    async def _discover_tools(self) -> None:
        """Discover available tools from the server."""
        if not self.session:
            raise RuntimeError("Not connected to server")

        try:
            tools_response = await self.session.list_tools()

            self.tools = []
            for tool in tools_response.tools:
                tool_info = MCPToolInfo(
                    name=tool.name,
                    description=tool.description or "",
                    input_schema=tool.inputSchema or {},
                    server_name=self.config.name,
                )
                self.tools.append(tool_info)

            logger.debug(
                "Discovered tools",
                server=self.config.name,
                tools=[t.name for t in self.tools],
            )

        except Exception as e:
            logger.error(
                "Failed to discover tools",
                server=self.config.name,
                error=str(e),
            )
            raise

    async def _discover_tools_http(self) -> None:
        """Discover available tools via HTTP transport."""
        try:
            self._message_id += 1
            request = {
                "jsonrpc": "2.0",
                "id": self._message_id,
                "method": "tools/list",
                "params": {}
            }

            response = await self._http_client.post(
                self._http_url,
                json=request,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            result = response.json()

            if "error" in result:
                msg = f"MCP error: {result['error']}"
                raise RuntimeError(msg)

            tools_data = result.get("result", {}).get("tools", [])
            self.tools = []

            for tool in tools_data:
                tool_info = MCPToolInfo(
                    name=tool.get("name", ""),
                    description=tool.get("description", ""),
                    input_schema=tool.get("inputSchema", {}),
                    server_name=self.config.name,
                )
                self.tools.append(tool_info)

            logger.debug(
                "Discovered tools via HTTP",
                server=self.config.name,
                tool_count=len(self.tools),
            )

        except Exception as e:
            logger.error(
                "Failed to discover tools via HTTP",
                server=self.config.name,
                error=str(e),
            )
            raise

    async def _call_tool_http(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        """Call a tool via HTTP transport.

        Args:
            tool_name: Name of the tool
            arguments: Tool arguments

        Returns:
            Tool result
        """
        self._message_id += 1
        request = {
            "jsonrpc": "2.0",
            "id": self._message_id,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments
            }
        }

        response = await self._http_client.post(
            self._http_url,
            json=request,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        result = response.json()

        if "error" in result:
            msg = f"MCP tool call failed: {result['error']}"
            raise RuntimeError(msg)

        return result.get("result", {})

    async def _discover_tools_with_retry(self, *, max_retries: int = 3) -> None:
        """Discover tools with retry logic for SSE connections.

        SSE connections may have timing issues where the endpoint event
        hasn't been received yet. This method retries tool discovery
        with exponential backoff.

        Args:
            max_retries: Maximum number of retry attempts
        """
        for attempt in range(max_retries):
            try:
                await self._discover_tools()
                if self.tools:  # Success if we got tools
                    return

                # Got empty tools list - might be timing issue
                if attempt < max_retries - 1:
                    wait_time = 0.5 * (2 ** attempt)
                    logger.warning(
                        "Tool discovery returned 0 tools, retrying",
                        server=self.config.name,
                        attempt=attempt + 1,
                        wait_time=wait_time,
                    )
                    await asyncio.sleep(wait_time)

            except Exception as e:
                if attempt == max_retries - 1:
                    raise

                wait_time = 0.5 * (2 ** attempt)
                logger.warning(
                    "Tool discovery failed, retrying",
                    server=self.config.name,
                    attempt=attempt + 1,
                    wait_time=wait_time,
                    error=str(e),
                )
                await asyncio.sleep(wait_time)

    async def call_tool(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        """Call a tool on this server.

        Args:
            tool_name: Name of the tool
            arguments: Tool arguments

        Returns:
            Tool result
        """
        logger.debug(
            "Calling MCP tool",
            server=self.config.name,
            tool=tool_name,
            arguments=arguments,
        )

        try:
            if self.config.transport == "http":
                result = await self._call_tool_http(tool_name, arguments)
                logger.debug("MCP tool call completed", server=self.config.name, tool=tool_name)

                # HTTP returns dict directly; unwrap text content when present.
                if isinstance(result, dict) and "content" in result:
                    content = result["content"]
                    if isinstance(content, list) and len(content) > 0:
                        content_item = content[0]
                        if isinstance(content_item, dict) and "text" in content_item:
                            return content_item["text"]
                return result

            # SSE/stdio transport uses session
            if not self.session:
                raise RuntimeError("Not connected to server")

            result = await self.session.call_tool(tool_name, arguments)

            logger.debug("MCP tool call completed", server=self.config.name, tool=tool_name)

            if hasattr(result, "content") and result.content and len(result.content) > 0:
                content_item = result.content[0]
                if hasattr(content_item, "text"):
                    return content_item.text
                return str(content_item)

            return str(result)

        except Exception as e:
            logger.error(
                "MCP tool call failed",
                server=self.config.name,
                tool=tool_name,
                error=str(e),
            )
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Signal the background task to disconnect and wait for it to finish."""
        logger.info("Disconnecting from MCP server", server=self.config.name)

        # Signal the background task to disconnect
        self._disconnect_event.set()

        # Wait for the background task to complete
        if self._connection_task:
            try:
                await self._connection_task
            except (asyncio.CancelledError, Exception) as e:
                logger.warning(
                    "Error during disconnect task completion",
                    server=self.config.name,
                    error=str(e),
                )

        # Clean up
        self.session = None
        self._connection_task = None

        logger.debug(
            "Disconnected from MCP server",
            server=self.config.name,
        )


class MCPRegistry:
    """Registry of all configured MCP servers.

    Connects to each server on startup, then optionally freezes (terminates
    subprocesses, retains tool schema snapshot) for process-lifetime sharing.
    """

    def __init__(self, config: CoreConfig) -> None:
        self.config = config
        self.connectors: dict[str, MCPServerConnector] = {}
        self._frozen: bool = False

        logger.debug("Initialized MCPRegistry")

    @property
    def frozen(self) -> bool:
        """True once subprocesses are shut down but the tool snapshot is retained."""
        return self._frozen

    # Bounded so a hanging stdio cleanup can't deadlock lifespan startup;
    # any pending __aexit__ work is cancelled on expiry and subprocesses
    # may leak (process exit reaps them).
    FREEZE_TIMEOUT_S = 10.0

    async def freeze(self) -> None:
        """Terminate stdio subprocesses while preserving each connector's ``tools``.

        After this returns, ``connect_all``/``disconnect_all`` are no-ops, so the
        instance is safe to share across Sessions. Idempotent.
        """
        if self._frozen:
            return

        try:
            await asyncio.wait_for(
                asyncio.gather(
                    *[
                        connector.__aexit__(None, None, None)
                        for connector in self.connectors.values()
                    ],
                    return_exceptions=True,
                ),
                timeout=self.FREEZE_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "MCP registry freeze timed out; pending __aexit__ tasks "
                "cancelled, some stdio subprocesses may be leaked until "
                "process exit",
                timeout_s=self.FREEZE_TIMEOUT_S,
                servers=len(self.connectors),
            )

        self._frozen = True

        total_tools = sum(len(c.tools) for c in self.connectors.values())
        logger.info(
            "MCP registry frozen",
            servers=len(self.connectors),
            tools=total_tools,
        )

    async def connect_all(self) -> None:
        """Connect to all configured MCP servers. Skips servers with enabled=False.

        No-op when the registry is frozen.
        """
        if self._frozen:
            return

        enabled_servers = [s for s in self.config.mcp.servers if s.enabled]
        disabled_count = len(self.config.mcp.servers) - len(enabled_servers)

        if disabled_count > 0:
            disabled_names = [s.name for s in self.config.mcp.servers if not s.enabled]
            logger.debug(
                "Skipping disabled MCP servers",
                disabled_servers=disabled_names,
            )

        logger.debug(
            "Connecting to MCP servers",
            server_count=len(enabled_servers),
        )

        for server_config in enabled_servers:
            connector = MCPServerConnector(server_config)
            self.connectors[server_config.name] = connector

        connector_names = list(self.connectors.keys())
        results = await asyncio.gather(
            *[self.connectors[name].__aenter__() for name in connector_names],
            return_exceptions=True,
        )

        # Drop connectors that failed to connect so a frozen snapshot never
        # contains a server with empty tools. Pre-refactor, a per-workspace
        # registry would retry on next workspace start; post-refactor, one bad
        # boot would otherwise degrade the process for its lifetime.
        failed: list[tuple[str, Exception]] = []
        for name, result in zip(connector_names, results, strict=True):
            if isinstance(result, Exception):
                failed.append((name, result))
                self.connectors.pop(name, None)

        if failed:
            logger.warning(
                "Some MCP servers failed to connect; dropped from registry",
                error_count=len(failed),
                failed_servers=[name for name, _ in failed],
                errors=[str(exc) for _, exc in failed],
            )

        logger.debug("MCP servers connected", servers=list(self.connectors.keys()))

    async def disconnect_all(self) -> None:
        """Exit all connector contexts in parallel. No-op when frozen."""
        if self._frozen:
            return

        logger.info("Disconnecting from all MCP servers")

        await asyncio.gather(
            *[
                connector.__aexit__(None, None, None)
                for connector in self.connectors.values()
            ],
            return_exceptions=True,
        )

        self.connectors.clear()

    async def _force_disconnect_all(self) -> None:
        """Tear down every connector regardless of ``_frozen`` state.

        For lifespan-startup error rollback, where a partially-frozen registry
        could otherwise leak its already-spawned subprocesses past the failure
        point. Do not call from normal Session paths — use ``disconnect_all``.
        """
        if not self.connectors:
            return

        await asyncio.gather(
            *[
                connector.__aexit__(None, None, None)
                for connector in self.connectors.values()
            ],
            return_exceptions=True,
        )
        self.connectors.clear()

    def get_all_tools(self) -> dict[str, list[MCPToolInfo]]:
        """Return tools grouped by server name."""
        tools_by_server = {}

        for server_name, connector in self.connectors.items():
            tools_by_server[server_name] = connector.tools

        return tools_by_server

    def get_tool_info(self, server_name: str, tool_name: str) -> MCPToolInfo | None:
        """Get information about a specific tool.

        Args:
            server_name: Name of the server
            tool_name: Name of the tool

        Returns:
            Tool info or None if not found
        """
        connector = self.connectors.get(server_name)
        if not connector:
            return None

        for tool in connector.tools:
            if tool.name == tool_name:
                return tool

        return None

    async def call_tool(
        self,
        server_name: str,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> Any:
        """Call a tool on a specific server.

        Args:
            server_name: Name of the server
            tool_name: Name of the tool
            arguments: Tool arguments

        Returns:
            Tool result
        """
        if self._frozen:
            raise RuntimeError(
                "call_tool unsupported on frozen MCPRegistry; "
                "route MCP calls via the sandbox-side cohort."
            )
        connector = self.connectors.get(server_name)
        if not connector:
            msg = f"Server not found: {server_name}"
            raise ValueError(msg)

        return await connector.call_tool(tool_name, arguments)

    async def __aenter__(self) -> "MCPRegistry":
        """Async context manager entry."""
        await self.connect_all()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Async context manager exit."""
        await self.disconnect_all()


# Process-global frozen registry installed at server lifespan startup
# (see src/server/app/setup.py). Sessions borrow this snapshot; when None,
# Session falls back to creating a per-instance registry (tests, standalone).
#
# TODO(option-c): eliminate the backend cohort by having the sandbox emit its
# own tool schemas at boot via a one-shot ``--describe`` mode.
_GLOBAL_REGISTRY: MCPRegistry | None = None


def get_global_registry() -> MCPRegistry | None:
    """Return the process-global frozen registry, or None if not installed."""
    return _GLOBAL_REGISTRY


def set_global_registry(registry: MCPRegistry) -> None:
    """Install the process-global registry. The registry must be frozen so
    Sessions borrowing it can rely on the snapshot invariant (no live stdio
    subprocesses, schemas are immutable for the process lifetime).
    """
    if not registry.frozen:
        raise ValueError(
            "Global MCP registry must be frozen before installing; call "
            "registry.freeze() first."
        )
    global _GLOBAL_REGISTRY
    _GLOBAL_REGISTRY = registry


def clear_global_registry() -> None:
    """Drop the process-global registry reference."""
    global _GLOBAL_REGISTRY
    _GLOBAL_REGISTRY = None
