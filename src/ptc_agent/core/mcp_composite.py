"""Per-workspace composite registry (append-only over the frozen built-ins).

A workspace's effective MCP set = the process-global built-ins (taken verbatim,
never round-tripped through the discovery cache) PLUS the workspace's
user-configured servers, whose tool schemas come from the sanitized discovery
snapshot. Nothing here executes a tool: the registry is a schema surface, and
every MCP call happens sandbox-side.

A zero-user-server workspace short-circuits to the built-in registry object
itself (identity), which is what keeps such workspaces byte-identical to the
pre-change behavior (manifest hash + prompt summary unchanged).
"""

from typing import TYPE_CHECKING, Any

from ptc_agent.config.core import MCPServerConfig

from .mcp_schema import MCPToolInfo

if TYPE_CHECKING:
    from .mcp_registry import MCPRegistry, MCPServerConnector


class SchemaOnlyRegistry:
    """Duck-typed ``MCPRegistry`` over built-in connectors + user-server schemas.

    Read-only: built-in tools come straight from the frozen global registry's
    connectors; user-server tools are wrapped ``MCPToolInfo`` from the sanitized
    discovery cache.
    """

    def __init__(
        self,
        builtin_registry: "MCPRegistry",
        user_servers: list[MCPServerConfig],
        tool_schemas: dict[str, list[dict]],
        disabled_builtin_names: frozenset[str] = frozenset(),
    ) -> None:
        self._builtin_registry = builtin_registry
        # Built-ins a workspace turned off: excluded from get_all_tools(),
        # connectors, and the effective config so the agent neither sees nor can
        # call them (a disable-marker must take effect at runtime, not just in
        # the resolver).
        self._disabled_builtin_names = frozenset(disabled_builtin_names)
        # User-server tools, in deterministic per-server order, wrapped as
        # MCPToolInfo so every downstream reader (codegen, formatter, hash) sees
        # the same shape as a built-in. Original tool names are preserved;
        # codegen re-sanitizes them.
        self._user_tools: dict[str, list[MCPToolInfo]] = {}
        for server in user_servers:
            schemas = tool_schemas.get(server.name)
            if not schemas:
                # Pending/error server: contributes config (so the prompt can
                # mention it) but zero tools.
                continue
            self._user_tools[server.name] = [
                MCPToolInfo(
                    name=schema.get("name", ""),
                    description=schema.get("description", "") or "",
                    input_schema=schema.get("input_schema") or {},
                    server_name=server.name,
                )
                for schema in schemas
            ]
        # Effective config: enabled built-ins (verbatim, minus disabled) + user
        # servers, so the formatter sees each user server's
        # description/instruction/source and never a workspace-disabled built-in.
        builtin_config = builtin_registry.config
        effective_builtins = [
            s
            for s in builtin_config.mcp.servers
            if s.name not in self._disabled_builtin_names
        ]
        # Shallow copy: every sub-config other than ``mcp`` stays the built-in
        # config's own object, so the composite is a faithful stand-in.
        self.config = builtin_config.model_copy(
            update={
                "mcp": builtin_config.mcp.model_copy(
                    update={"servers": [*effective_builtins, *user_servers]}
                )
            }
        )

    @property
    def frozen(self) -> bool:
        """Always frozen — a schema-only snapshot has no live subprocesses."""
        return True

    @property
    def connectors(self) -> dict[str, "MCPServerConnector"]:
        """Built-in connectors ONLY — user servers have no host connector, ever.

        Workspace-disabled built-ins are excluded so a disabled built-in's
        connector is neither visible nor callable.
        """
        connectors = self._builtin_registry.connectors
        if not self._disabled_builtin_names:
            return connectors
        return {
            name: conn
            for name, conn in connectors.items()
            if name not in self._disabled_builtin_names
        }

    def get_all_tools(self) -> dict[str, list[MCPToolInfo]]:
        """Enabled built-in tools (minus disabled), then user-server tools."""
        tools_by_server: dict[str, list[MCPToolInfo]] = {
            name: tools
            for name, tools in self._builtin_registry.get_all_tools().items()
            if name not in self._disabled_builtin_names
        }
        tools_by_server.update(self._user_tools)
        return tools_by_server

    def get_tool_info(self, server_name: str, tool_name: str) -> MCPToolInfo | None:
        """Look up a tool by server + name across built-ins and user servers."""
        if server_name in self._disabled_builtin_names:
            return None
        if server_name in self._user_tools:
            for tool in self._user_tools[server_name]:
                if tool.name == tool_name:
                    return tool
            return None
        return self._builtin_registry.get_tool_info(server_name, tool_name)


def build_composite_registry(
    builtin_registry: "MCPRegistry",
    user_servers: list[MCPServerConfig],
    tool_schemas: dict[str, list[dict]],
    disabled_builtin_names: frozenset[str] = frozenset(),
) -> Any:
    """Append user-server schemas onto the frozen built-in registry.

    ``user_servers`` are the untrusted (``source`` 'workspace' or 'user'),
    enabled servers the CALLER selected — the filter lives in
    ``WorkspaceManager``, not here — in resolver order (built-ins
    config-order, then user servers alphabetical). ``tool_schemas``
    maps a server name to its sanitized ``[{name, description, input_schema}]``
    snapshot; only ``status='ok'`` servers appear. ``disabled_builtin_names``
    are built-ins a workspace turned off — excluded from tools/connectors/config
    so the agent can't see or call them at runtime.

    When there are NO user servers AND no disabled built-ins, the built-in
    registry is returned UNCHANGED (identity), keeping clean workspaces
    byte-identical downstream.
    """
    if not user_servers and not disabled_builtin_names:
        return builtin_registry
    return SchemaOnlyRegistry(
        builtin_registry, user_servers, tool_schemas, disabled_builtin_names
    )
