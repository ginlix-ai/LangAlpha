"""MCP Server Registry - Connect to and manage external MCP servers.

Connection lifecycle only. Schema translation lives in :mod:`mcp_schema`,
failure diagnosis in :mod:`mcp_diagnostics`, and the per-workspace composite in
:mod:`mcp_composite`; all three are re-exported here because this module is the
documented import surface.
"""

import asyncio
import os
import random
from types import TracebackType
from typing import Any

import httpx2
import structlog
from mcp import StdioServerParameters
from mcp.client import Client
from mcp.client.sse import sse_client
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client

# The SDK's own httpx factory (sse_client's default); no public re-export yet.
from mcp.shared._httpx_utils import create_mcp_http_client

from ptc_agent.config.core import CoreConfig, MCPServerConfig
from src.observability.tracing import tracer as _otel_tracer

from .mcp_composite import SchemaOnlyRegistry, build_composite_registry
from .mcp_diagnostics import StderrTail, classify_remote_failure, classify_startup_failure
from .mcp_schema import MCPToolInfo, client_identity

logger = structlog.get_logger(__name__)

__all__ = [
    "MCPRegistry",
    "MCPServerConnector",
    "MCPToolInfo",
    "SchemaOnlyRegistry",
    "build_composite_registry",
    "classify_startup_failure",
    "clear_global_registry",
    "get_global_registry",
    "reconnect_builtin_remote",
    "set_global_registry",
]


class MCPServerConnector:
    """Connector for an individual MCP server.

    A background task holds the transport + ``Client`` contexts alive for the
    connection's lifetime; ``Client`` (mode="auto") negotiates the protocol era
    per connection — ``server/discover`` probe with legacy ``initialize``
    fallback.
    """

    def __init__(self, config: MCPServerConfig, *, quiet: bool = False) -> None:
        self.config = config
        # A background reconnect logs one line of its own per attempt, so the
        # per-failure warning (and a 4xx's traceback) would repeat it every
        # minute for as long as the server stays down.
        self._quiet = quiet
        self.session: Client | None = None
        self.tools: list[MCPToolInfo] = []
        # The server's own business card from the handshake, as the wire spelled
        # it, so a builtin and a sandbox-discovered server hand the UI the same
        # shape. Survives disconnect for the same reason ``tools`` does: this
        # process asked once at startup and the answer is what it serves.
        self.server_info: dict[str, Any] | None = None

        # Background task management
        self._connection_task: asyncio.Task | None = None
        self._ready: asyncio.Event = asyncio.Event()
        self._disconnect_event: asyncio.Event = asyncio.Event()
        self._connection_error: Exception | None = None
        # Human diagnosis of a startup failure; connect_all logs this instead
        # of the exception's opaque TaskGroup repr.
        self.failure_reason: str | None = None
        # Whether connecting again could succeed (a remote server restarting),
        # as opposed to a fault every attempt would repeat.
        self.failure_transient: bool = False
        self._last_http_status: int | None = None

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
        """Safe base vars plus the server's declared ``env:``, placeholders expanded.

        Starts from a safe subset of os.environ (not the full environment) to
        prevent leaking host secrets to MCP server subprocesses.
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
        await self.connect()
        return self

    async def connect(self, timeout: float | None = None) -> None:
        """Connect and discover tools, raising the failure if there is one.

        ``timeout`` bounds the whole attempt; on expiry the connection task is
        cancelled, since a server that accepts and never answers would
        otherwise hold it for the transport's own multi-minute read timeout.
        """
        logger.debug("Connecting to MCP server", server=self.config.name)

        # Start background task that keeps nested contexts alive
        self._connection_task = asyncio.create_task(
            self._run_connection(), name=f"mcp-{self.config.name}"
        )

        try:
            await asyncio.wait_for(self._ready.wait(), timeout)
        except TimeoutError:
            await self._abandon_connection()
            self._connection_error = TimeoutError(f"no answer within {timeout:.1f}s")
            self.failure_reason = f"server did not answer within {timeout:.1f}s"
            self.failure_transient = True
            (logger.debug if self._quiet else logger.warning)(
                "Failed to connect to MCP server",
                server=self.config.name,
                diagnosis=self.failure_reason,
            )
        except asyncio.CancelledError:
            # Shutdown cancelling a background reconnect mid-attempt; the
            # attempt's task would otherwise outlive the loop it runs on.
            await self._abandon_connection()
            raise

        if self._connection_error:
            raise self._connection_error

        logger.debug(
            "Connected to MCP server",
            server=self.config.name,
            tool_count=len(self.tools),
        )

    # Cancelling an http connection ends its session with a DELETE on the
    # transport's 300 s read timeout. A server that stalls it must not hold boot
    # or shutdown past the attempt's own deadline, so teardown gets this long
    # before a second cancel interrupts the DELETE itself.
    ABANDON_GRACE_S = 2.0

    async def _abandon_connection(self) -> None:
        assert self._connection_task is not None
        self._disconnect_event.set()
        task = self._connection_task
        for _ in range(2):
            task.cancel()
            done, _ = await asyncio.wait({task}, timeout=self.ABANDON_GRACE_S)
            if done:
                break
        if task.done() and not task.cancelled():
            task.exception()  # retrieved; the attempt records its own failure

    def _resolve_headers(self) -> dict[str, str] | None:
        """Config headers with ``${VAR}`` values expanded; None when empty."""
        if not self.config.headers:
            return None
        return {k: os.path.expandvars(v) for k, v in self.config.headers.items()}

    def _tracks(self, request: httpx2.Request) -> bool:
        # Streamable HTTP's GET listen stream and DELETE teardown are optional
        # and many servers refuse them, so only the POSTs discovery depends on
        # say why a connection failed. Every SSE request does.
        return self.config.transport != "http" or request.method == "POST"

    async def _note_request(self, request: httpx2.Request) -> None:
        # A status speaks only for the request it answered: an older server's
        # 400 to the version probe must not name an initialize that then got
        # no answer at all. Discovery sends its requests one at a time.
        if self._tracks(request):
            self._last_http_status = None

    async def _note_response(self, response: httpx2.Response) -> None:
        if self._tracks(response.request):
            self._last_http_status = response.status_code

    def _http_client(
        self,
        headers: dict[str, str] | None = None,
        timeout: httpx2.Timeout | None = None,
        auth: httpx2.Auth | None = None,
    ) -> httpx2.AsyncClient:
        """The SDK's client, with a hook keeping the status its errors drop."""
        client = create_mcp_http_client(headers=headers, timeout=timeout, auth=auth)
        client.event_hooks["request"].append(self._note_request)
        client.event_hooks["response"].append(self._note_response)
        return client

    async def _serve(self, client: Client, *, retry_discovery: bool = False) -> None:
        """Discover tools, signal readiness, and hold the connection open."""
        self.session = client
        self.server_info = client_identity(client)
        if retry_discovery:
            await self._discover_tools_with_retry()
        else:
            await self._discover_tools()

        logger.debug(
            "MCP connection established",
            server=self.config.name,
            transport=self.config.transport,
        )

        # Signal that connection is ready, then keep contexts alive until
        # disconnect is signaled.
        self._ready.set()
        await self._disconnect_event.wait()

        logger.debug(
            "MCP connection disconnect signaled",
            server=self.config.name,
        )

    async def _run_connection(self) -> None:
        """Background task that maintains the nested async with contexts.

        Contexts are entered and exited in LIFO order within this single task
        (MCP SDK best practice); the transport variants differ only in how the
        stream contexts are built.
        """
        stderr_capture: StderrTail | None = None
        try:
            if self.config.transport == "http":
                url = self._expand_url()
                if not url:
                    msg = f"URL required for HTTP transport: {self.config.name}"
                    raise ValueError(msg)

                # Custom headers ride on a preconfigured httpx client;
                # streamable_http_client takes no headers of its own.
                async with self._http_client(headers=self._resolve_headers()) as http_client:
                    async with Client(
                        streamable_http_client(url, http_client=http_client)
                    ) as client:
                        await self._serve(client)

            elif self.config.transport == "sse":
                url = self._expand_url()
                if not url:
                    msg = f"URL required for SSE transport: {self.config.name}"
                    raise ValueError(msg)

                # SSE connections need discovery retry due to endpoint event
                # timing. Headers are resolved exactly as on the http arm — an
                # authenticated SSE server 401s without them.
                async with Client(
                    sse_client(
                        url,
                        headers=self._resolve_headers(),
                        httpx_client_factory=self._http_client,
                    )
                ) as client:
                    await self._serve(client, retry_discovery=True)

            else:
                # Stdio transport (default) - use command-based connection
                if not self.config.command:
                    raise ValueError("Command is required for stdio transport")
                server_params = StdioServerParameters(
                    command=self.config.command,
                    args=self.config.args,
                    env=self._prepare_env(),
                )

                stderr_capture = StderrTail()
                async with Client(
                    stdio_client(server_params, errlog=stderr_capture.writer)
                ) as client:
                    await self._serve(client)

        except Exception as e:
            # Store error and signal ready (below, once it is classified) so
            # connect() can raise it
            self._connection_error = e

            import traceback
            error_details = traceback.format_exc()

            stderr_tail = ""
            if stderr_capture is not None:
                # Close the writer and join the drain thread before reading, so
                # the tail can't race the daemon still draining the dying
                # subprocess's traceback into the deque.
                stderr_tail = stderr_capture.tail(drain=True)

            if self.config.transport == "stdio":
                self.failure_reason = classify_startup_failure(e, stderr_tail) or str(e)
            else:
                self.failure_reason, self.failure_transient = classify_remote_failure(
                    e, self._last_http_status
                )
            self._ready.set()

            if self.failure_transient:
                # A server that is down or restarting; the diagnosis says all a
                # traceback would, and the caller decides whether it is final.
                (logger.debug if self._quiet else logger.warning)(
                    "Failed to connect to MCP server",
                    server=self.config.name,
                    diagnosis=self.failure_reason,
                )
                return
            (logger.debug if self._quiet else logger.error)(
                "Failed to connect to MCP server",
                server=self.config.name,
                error=str(e),
                error_type=type(e).__name__,
                diagnosis=self.failure_reason,
                traceback=error_details,
                stderr_tail=stderr_tail or None,
            )
        finally:
            if stderr_capture is not None:
                stderr_capture.close()

    async def _discover_tools(self) -> None:
        """Discover available tools from the server."""
        if not self.session:
            raise RuntimeError("Not connected to server")

        span = _otel_tracer.start_span(
            "mcp.discover", attributes={"server": self.config.name}
        )

        try:
            tools_response = await self.session.list_tools()

            self.tools = []
            for tool in tools_response.tools:
                tool_info = MCPToolInfo(
                    name=tool.name,
                    description=tool.description or "",
                    input_schema=tool.input_schema or {},
                    server_name=self.config.name,
                )
                self.tools.append(tool_info)

            logger.debug(
                "Discovered tools",
                server=self.config.name,
                tools=[t.name for t in self.tools],
            )

            span.set_attribute("tool_count", len(self.tools))

        except Exception as e:
            logger.error(
                "Failed to discover tools",
                server=self.config.name,
                error=str(e),
            )
            span.record_exception(e)
            raise
        finally:
            span.end()

    async def _discover_tools_with_retry(self, *, max_retries: int = 3) -> None:
        """Discover tools with exponential backoff, for SSE only.

        An SSE connection can be usable before its endpoint event arrives, so
        the first list_tools may legitimately come back empty.
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
        # Built-in remote servers that never connected, name -> diagnosis. The
        # process serves without their tools until a background reconnect
        # publishes a successor without the entry, so this is its readiness
        # answer; it says nothing about any other worker.
        self.failed_builtin_remote: dict[str, str] = {}

        logger.debug("Initialized MCPRegistry")

    @property
    def frozen(self) -> bool:
        """True once subprocesses are shut down but the tool snapshot is retained."""
        return self._frozen

    # Bounded so a hanging stdio cleanup can't deadlock lifespan startup;
    # any pending __aexit__ work is cancelled on expiry and subprocesses
    # may leak (process exit reaps them).
    FREEZE_TIMEOUT_S = 10.0

    async def _exit_all_connectors(self) -> None:
        """Exit every connector context in parallel, absorbing their failures."""
        await asyncio.gather(
            *[
                connector.__aexit__(None, None, None)
                for connector in self.connectors.values()
            ],
            return_exceptions=True,
        )

    async def freeze(self) -> None:
        """Terminate stdio subprocesses while preserving each connector's ``tools``.

        After this returns, ``connect_all``/``disconnect_all`` are no-ops, so the
        instance is safe to share across Sessions. Idempotent.
        """
        if self._frozen:
            return

        try:
            await asyncio.wait_for(
                self._exit_all_connectors(), timeout=self.FREEZE_TIMEOUT_S
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

    # A built-in remote server is part of the deployment, so at boot one that
    # is restarting is waited for rather than dropped for the process's
    # lifetime. A refused connection is retried at about 2, 6, 14 and 22 s; a
    # server that accepts and hangs spends at most the per-attempt cap, and
    # nothing runs past the budget.
    BUILTIN_REMOTE_ATTEMPTS = 5
    BUILTIN_REMOTE_BUDGET_S = 30.0
    BUILTIN_REMOTE_ATTEMPT_TIMEOUT_S = 10.0
    BUILTIN_REMOTE_BACKOFF_S = 2.0
    BUILTIN_REMOTE_MAX_BACKOFF_S = 8.0

    @staticmethod
    def _is_builtin_remote(config: MCPServerConfig) -> bool:
        return config.source == "builtin" and config.transport in ("http", "sse")

    async def _connect_with_retry(self, name: str) -> None:
        """Connect a built-in remote server, retrying transient failures.

        Each attempt replaces ``self.connectors[name]`` with a fresh connector,
        so a concurrent disconnect always reaches the attempt in flight. A 4xx
        is not retried: auth and configuration faults do not heal by waiting.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.BUILTIN_REMOTE_BUDGET_S
        connector = self.connectors[name]
        for attempt in range(1, self.BUILTIN_REMOTE_ATTEMPTS + 1):
            try:
                await connector.connect(
                    timeout=max(
                        min(deadline - loop.time(), self.BUILTIN_REMOTE_ATTEMPT_TIMEOUT_S), 0.1
                    )
                )
                if attempt > 1:
                    logger.info("Connected to MCP server after retrying", server=name, attempt=attempt)
                return
            except Exception:
                delay = min(
                    self.BUILTIN_REMOTE_BACKOFF_S * 2 ** (attempt - 1),
                    self.BUILTIN_REMOTE_MAX_BACKOFF_S,
                )
                if (
                    not connector.failure_transient
                    or attempt == self.BUILTIN_REMOTE_ATTEMPTS
                    or loop.time() + delay >= deadline
                ):
                    raise
                logger.warning(
                    "Built-in MCP server unavailable, retrying",
                    server=name,
                    attempt=attempt,
                    retry_in_s=delay,
                    diagnosis=connector.failure_reason,
                )
            await asyncio.sleep(delay)
            connector = MCPServerConnector(connector.config)
            self.connectors[name] = connector

    async def connect_all(self, *, retry_builtin_remote: bool = False) -> None:
        """Connect to all configured MCP servers. Skips servers with enabled=False.

        ``retry_builtin_remote`` is for boot discovery, where a server still
        down after the budget is left to :func:`reconnect_builtin_remote`; a
        per-session registry fails fast instead of holding its session start
        for the retry budget.

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
            *[
                self._connect_with_retry(name)
                if retry_builtin_remote and self._is_builtin_remote(self.connectors[name].config)
                else self.connectors[name].__aenter__()
                for name in connector_names
            ],
            return_exceptions=True,
        )

        # Drop connectors that failed to connect so a frozen snapshot never
        # contains a server with empty tools. Pre-refactor, a per-workspace
        # registry would retry on next workspace start; post-refactor, one bad
        # boot would otherwise degrade the process for its lifetime.
        failed: list[tuple[str, str]] = []
        for name, result in zip(connector_names, results, strict=True):
            if isinstance(result, Exception):
                connector = self.connectors.pop(name, None)
                reason = getattr(connector, "failure_reason", None) or str(result)
                failed.append((name, reason))
                if connector is not None and self._is_builtin_remote(connector.config):
                    self.failed_builtin_remote[name] = reason

        if failed:
            logger.warning(
                "Some MCP servers failed to connect; dropped from registry",
                error_count=len(failed),
                failed_servers=[name for name, _ in failed],
                errors=[reason for _, reason in failed],
            )
        if self.failed_builtin_remote:
            logger.error(
                "Built-in remote MCP servers unavailable; serving without their tools",
                servers=sorted(self.failed_builtin_remote),
                errors=list(self.failed_builtin_remote.values()),
            )

        logger.debug("MCP servers connected", servers=list(self.connectors.keys()))

    async def disconnect_all(self) -> None:
        """Exit all connector contexts in parallel. No-op when frozen."""
        if self._frozen:
            return

        logger.info("Disconnecting from all MCP servers")

        await self._exit_all_connectors()
        self.connectors.clear()

    async def _force_disconnect_all(self) -> None:
        """Tear down every connector regardless of ``_frozen`` state.

        For lifespan-startup error rollback, where a partially-frozen registry
        could otherwise leak its already-spawned subprocesses past the failure
        point. Do not call from normal Session paths — use ``disconnect_all``.
        """
        if not self.connectors:
            return

        await self._exit_all_connectors()
        self.connectors.clear()

    def _succeeded_by(self, connector: MCPServerConnector) -> "MCPRegistry":
        """A frozen copy with ``connector``'s server reconnected.

        Servers keep config order, which is boot order, because the prompt's
        tool summary and the sandbox wrappers are rendered in registry order.
        """
        name = connector.config.name
        connectors = {**self.connectors, name: connector}
        successor = MCPRegistry(self.config)
        successor.connectors = {
            s.name: connectors[s.name] for s in self.config.mcp.servers if s.name in connectors
        }
        successor.failed_builtin_remote = {
            k: v for k, v in self.failed_builtin_remote.items() if k != name
        }
        successor._frozen = True
        return successor

    def get_all_tools(self) -> dict[str, list[MCPToolInfo]]:
        """Return tools grouped by server name."""
        return {name: c.tools for name, c in self.connectors.items()}

    def get_tool_info(self, server_name: str, tool_name: str) -> MCPToolInfo | None:
        connector = self.connectors.get(server_name)
        if not connector:
            return None
        return next((t for t in connector.tools if t.name == tool_name), None)

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
# A background reconnect replaces it with a successor; a snapshot already
# handed out never changes.
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
    subprocesses, a snapshot's schemas never change). A heal installs a new
    snapshot instead, which is how a borrower tells its view is stale.
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


# Built-in remote servers still down after boot's retry budget are retried for
# as long as the process runs: about 5, 10, 20 and 40 s apart, then every
# minute, jittered so the workers of a fleet do not reconnect in lockstep.
HEAL_INITIAL_DELAY_S = 5.0
HEAL_MAX_DELAY_S = 60.0
HEAL_JITTER = 0.2


async def reconnect_builtin_remote() -> None:
    """Reconnect this worker's failed built-in remote servers, each on its own.

    A server that answers is published as it would have been at boot: its
    tools frozen into a successor of the global registry, without its
    ``failed_builtin_remote`` entry. Runs until none is left or it is
    cancelled. A 4xx is retried too, at the slow rate, because the fault is
    usually the other side's deployment and fixing it should not need this
    process restarted.
    """
    registry = get_global_registry()
    if registry is None or not registry.failed_builtin_remote:
        return
    configs = [s for s in registry.config.mcp.servers if s.name in registry.failed_builtin_remote]
    try:
        async with asyncio.TaskGroup() as group:
            for config in configs:
                group.create_task(_reconnect_one(config), name=f"mcp-reconnect-{config.name}")
    except Exception:
        # Only a bug gets here; without the log the worker would stay degraded
        # with nothing saying why the reconnects stopped.
        logger.exception("Background reconnect of built-in MCP servers stopped")


async def _reconnect_one(config: MCPServerConfig) -> None:
    loop = asyncio.get_running_loop()
    started = loop.time()
    delay = HEAL_INITIAL_DELAY_S
    last_reason: str | None = None
    attempt = 0
    while True:
        await asyncio.sleep(delay * random.uniform(1 - HEAL_JITTER, 1 + HEAL_JITTER))
        current = get_global_registry()
        if current is None or config.name not in current.failed_builtin_remote:
            return
        attempt += 1
        connector = MCPServerConnector(config, quiet=True)
        try:
            await connector.connect(timeout=MCPRegistry.BUILTIN_REMOTE_ATTEMPT_TIMEOUT_S)
        except Exception as e:
            reason = connector.failure_reason or str(e)
            transient = connector.failure_transient
            delay = min(delay * 2, HEAL_MAX_DELAY_S) if transient else HEAL_MAX_DELAY_S
            if attempt == 1:
                log = logger.warning
            elif reason != last_reason:
                log = logger.info
            else:
                log = logger.debug
            log(
                "Built-in MCP server still unavailable; retrying in the background",
                server=config.name,
                attempt=attempt,
                retry_in_s=delay,
                diagnosis=reason,
            )
            last_reason = reason
            continue

        # Freeze it as boot does: the session closes, the tool list stays.
        try:
            await asyncio.wait_for(
                connector.__aexit__(None, None, None), timeout=MCPRegistry.FREEZE_TIMEOUT_S
            )
        except TimeoutError:
            logger.warning(
                "Reconnected MCP server did not close in time; keeping its tools",
                server=config.name,
                timeout_s=MCPRegistry.FREEZE_TIMEOUT_S,
            )
        # Re-read after the await: another server's heal may have published a
        # successor meanwhile, and the swap below must build on it. Nothing
        # between the read and the install yields, so the swap is atomic.
        current = get_global_registry()
        if current is None or config.name not in current.failed_builtin_remote:
            return
        successor = current._succeeded_by(connector)
        set_global_registry(successor)
        logger.info(
            "Built-in MCP server reconnected",
            server=config.name,
            tools=len(connector.tools),
            attempts=attempt,
            retried_for_s=round(loop.time() - started, 1),
            still_failed=sorted(successor.failed_builtin_remote),
        )
        return
