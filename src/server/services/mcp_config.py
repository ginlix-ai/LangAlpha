"""Per-workspace MCP configuration resolution — the single chokepoint.

Modeled on ``resolve_llm_config``. Merges the process-global built-in MCP
servers (from ``base_config.mcp.servers``), the user's enabled user-level
servers, and a workspace's DB-backed rows into one deterministic effective set:

    effective = built-ins (config order)
                MINUS names disabled by a (source='builtin', enabled=false) row
                MINUS names in user_mcp_builtin_disables (account-wide)
                PLUS  enabled user-level servers (alphabetical)
                MINUS names disabled by a (source='user', enabled=false) row
                MINUS names shadowed by a workspace-local server
                PLUS  source='workspace' enabled rows (alphabetical, appended)

Collision policy: built-in names are reserved (a user or workspace server can
never shadow one); a workspace-local server shadows an inherited user server
of the same name (the explicit local-fork affordance).

User-level mutations bump every workspace of the user (one transaction), so
the single per-workspace ``mcp_config_version`` remains the only drift signal
sessions have to watch.

The merged list and the DB↔model converters are defined ONCE here so the API
effective-list endpoint and the sandbox-sync path can import the same logic
(no prompt/wrapper divergence).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from functools import cached_property
from typing import Literal
from urllib.parse import urlsplit

from ptc_agent.config.core import MCPServerConfig
from src.server.database.mcp_oauth import ConnectionStatus

_DEFAULT_PORTS = {"http": 80, "https": 443}


def _canonical_server_url(url: str | None) -> str:
    """Normalize an MCP endpoint for consent comparison.

    Case-folds scheme/host, drops the default port, and trims a trailing path
    slash so ``https://H/mcp`` and ``https://h:443/mcp/`` compare equal. Query
    is kept — it can select a different endpoint. Anything unparseable folds to
    the raw string, so a broken URL never accidentally matches a real one.
    """
    if not url:
        return ""
    try:
        parts = urlsplit(url.strip())
        port = parts.port  # raises for out-of-range or non-numeric ports
    except ValueError:
        return url.strip()
    if not parts.scheme or not parts.hostname:
        return url.strip()
    scheme = parts.scheme.lower()
    host = parts.hostname.lower()
    if ":" in host:
        # IPv6 literal (urlsplit strips the brackets) — restore them, or the
        # host/port boundary becomes ambiguous and distinct endpoints collide.
        host = f"[{host}]"
    netloc = host if port in (None, _DEFAULT_PORTS.get(scheme)) else f"{host}:{port}"
    path = parts.path.rstrip("/")
    query = f"?{parts.query}" if parts.query else ""
    return f"{scheme}://{netloc}{path}{query}"


def same_consented_url(a: str | None, b: str | None) -> bool:
    """Whether two URLs address the same consented endpoint (see canonicalizer)."""
    return _canonical_server_url(a) == _canonical_server_url(b)

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")


# Vault-reference resolution (``${vault:NAME}``) happens in-sandbox in Phase 2,
# not here — this module is the merge/convert chokepoint only. The canonical
# pattern lives in ``ptc_agent.core.mcp_sanitize.VAULT_REF_RE`` (Lane A); the
# Phase 2 secret-resolution codegen should import it from there.


class Origin(StrEnum):
    """Which tier defined a server. Matches the wire value of ``origin``."""

    BUILTIN = "builtin"
    WORKSPACE = "workspace"
    USER = "user"


class State(StrEnum):
    """How a server participates in one workspace's effective set.

    Only ``ACTIVE`` servers run. The other three are carried so the API can
    render a re-enable affordance (``DISABLED``/``TOMBSTONED``) or flag the
    local fork that hides an inherited server (``SHADOWED``).
    """

    ACTIVE = "active"
    DISABLED = "disabled"
    TOMBSTONED = "tombstoned"
    SHADOWED = "shadowed"


@dataclass(frozen=True)
class ResolvedServer:
    """One server in a workspace's resolved set, with its partition labels."""

    config: MCPServerConfig
    origin: Origin
    state: State
    # OAuth connection status INCLUDING revoked (unlike the config's
    # ``oauth_connection_id``, which only binds live connections) — lets
    # consumers tell "never OAuth" from "OAuth but disconnected". Only ever
    # set on ``USER``-origin entries.
    oauth_status: ConnectionStatus | None = None
    # DISABLED built-ins only: whether the disable came from this workspace's
    # marker row or the account-wide user disable. A scalar, not a new State —
    # the enums are locked and both disables render the same, only the copy
    # (and which toggle can undo it) differs.
    disabled_scope: Literal["workspace", "user"] | None = None
    # USER-origin rows installed by a plugin: the owning plugin's name,
    # display only. Rides here rather than MCPServerConfig — provenance must
    # never enter the config blob round-trip.
    plugin_name: str | None = None

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def host_side_oauth(self) -> bool:
        """Whether this server's tools are discovered host-side, never in-sandbox.

        True for a DISCONNECTED OAuth server too: ``oauth_connection_id`` is
        None once revoked, but the sandbox still holds no vendor token, so a
        probe from there could only cache a junk failure.
        """
        return bool(self.config.oauth_connection_id) or (
            self.origin is Origin.USER and self.oauth_status is not None
        )


@dataclass(frozen=True)
class ResolvedMCP:
    """The effective MCP server set for one workspace at one config version.

    ``entries`` is the single source of truth: one labelled row per server the
    workspace knows about, ordered so that filtering to ``ACTIVE`` yields the
    effective run order (built-ins in config order, then inherited, then local
    — both alphabetical). The projections below are derived from it, so the
    partition can never disagree with itself. ``version`` is
    ``workspaces.mcp_config_version``.
    """

    entries: tuple[ResolvedServer, ...]
    version: int

    def _names(self, origin: Origin, state: State) -> frozenset[str]:
        return frozenset(
            e.name for e in self.entries if e.origin is origin and e.state is state
        )

    @cached_property
    def servers(self) -> list[MCPServerConfig]:
        """The effective (running) set, in deterministic order."""
        return [e.config for e in self.entries if e.state is State.ACTIVE]

    @cached_property
    def disabled_builtin_names(self) -> frozenset[str]:
        return self._names(Origin.BUILTIN, State.DISABLED)

    @cached_property
    def shadowed_inherited_names(self) -> frozenset[str]:
        return self._names(Origin.USER, State.SHADOWED)


@dataclass(frozen=True)
class ServerRef:
    """A workspace-addressable MCP name, classified for the mutation endpoints.

    ``row`` is the workspace row for local/tombstone/marker refs and the
    Plugins row for a live inherited one — whichever tier the ref resolved
    from.
    """

    name: str
    origin: Origin
    state: State
    row: dict | None = None


def builtin_names() -> set[str]:
    """Names of the process-global built-in MCP servers (from agent_config).

    Built-in names are reserved across every tier, so this is the one place
    that reads them — the routers and the resolver must agree on the set.
    """
    from src.server.app import setup

    if setup.agent_config is None:
        return set()
    return {s.name for s in setup.agent_config.mcp.servers}


def workspace_row_to_server_config(row: dict) -> MCPServerConfig:
    """Convert a ``workspace_mcp_servers`` row into an ``MCPServerConfig``.

    Defined ONCE; imported by the API and sandbox-sync lanes. ``source`` is
    forced to ``"workspace"`` and any stored ``vault_blueprints`` key is
    stripped (defense in depth — user servers never declare blueprints).
    """
    config = dict(row.get("config") or {})
    config.pop("vault_blueprints", None)
    config.pop("source", None)  # never trust a stored source tag
    # A resolution OUTPUT, never an input: a stored blob must not be able to
    # bind itself to someone's OAuth connection.
    config.pop("oauth_connection_id", None)
    # The row's name is authoritative over any name baked into the JSON blob.
    config["name"] = row["name"]
    config["source"] = "workspace"
    config["enabled"] = bool(row.get("enabled", True))
    return MCPServerConfig(**config)


def user_row_to_server_config(
    row: dict, *, oauth_connection_id: str | None = None
) -> MCPServerConfig:
    """Convert a ``user_mcp_servers`` row (flat columns, no config blob) into
    an ``MCPServerConfig`` with ``source='user'``."""
    return MCPServerConfig(
        name=row["name"],
        enabled=True,
        description=row.get("description") or "",
        instruction=row.get("instruction") or "",
        transport=row.get("transport") or "stdio",
        command=row.get("command"),
        args=row.get("args") or [],
        env=row.get("env") or {},
        url=row.get("url"),
        headers=row.get("headers") or {},
        tool_exposure_mode=row.get("tool_exposure_mode") or None,
        source="user",
        discovery_uses_secrets=bool(row.get("discovery_uses_secrets", False)),
        oauth_connection_id=oauth_connection_id,
    )


async def classify_server_name(
    workspace_id: str, user_id: str, name: str
) -> ServerRef | None:
    """Classify one MCP name for a workspace mutation, or ``None`` if unknown.

    Reads the two mutable tiers directly (workspace rows, then the Plugins
    catalog) rather than going through ``resolve_mcp_config`` — mutations need
    the raw row, not the merged set, and the built-in tier is checked by the
    caller against the process config. A workspace-local row wins over an
    inherited server of the same name (it is the fork); a stale
    ``source='builtin'`` marker only classifies as ``BUILTIN`` once the
    Plugins tier has been ruled out.
    """
    from src.server.database.mcp_servers import (
        get_catalog_server,
        list_workspace_servers,
    )

    rows = {r["name"]: r for r in await list_workspace_servers(workspace_id)}
    row = rows.get(name)
    source = (row or {}).get("source")
    if source == "workspace":
        state = State.ACTIVE if row["enabled"] else State.DISABLED
        return ServerRef(name, Origin.WORKSPACE, state, row)
    if source == "user":
        # A (source='user', enabled=false) marker: this workspace's tombstone
        # for an inherited server.
        return ServerRef(name, Origin.USER, State.TOMBSTONED, row)

    catalog = await get_catalog_server(user_id, name)
    if catalog and catalog.get("enabled"):
        return ServerRef(name, Origin.USER, State.ACTIVE, catalog)
    if source == "builtin":
        return ServerRef(name, Origin.BUILTIN, State.DISABLED, row)
    return None


async def resolve_mcp_config(
    base_config,
    user_id: str,
    workspace_id: str,
) -> ResolvedMCP:
    """Resolve the effective MCP server set for ``workspace_id``.

    Built-ins come from ``base_config.mcp.servers`` (enabled ones, config
    order); a ``(source='builtin', enabled=false)`` row or an account-wide
    ``user_mcp_builtin_disables`` row removes a built-in by name; enabled
    user-level servers are inherited (alphabetical) unless tombstoned by a
    ``(source='user', enabled=false)`` row or shadowed by a workspace-local
    server; ``source='workspace'`` enabled rows are appended alphabetically.
    A workspace with zero rows AND zero user-level state returns the built-in
    objects unchanged (no copies) so the common case stays byte-identical
    downstream.
    """
    import asyncio

    from src.server.database.mcp_oauth import list_connections
    from src.server.database.mcp_servers import (
        get_workspace_servers_and_version,
        list_enabled_user_servers,
        list_user_builtin_disables,
    )

    # Built-ins from the global config, enabled only, in declaration order.
    builtin_servers = [
        s for s in base_config.mcp.servers
        if getattr(s, "enabled", True)
    ]
    builtin_name_set = {s.name for s in builtin_servers}

    # Version is read BEFORE the rows (READ COMMITTED, not a snapshot) so a
    # concurrent mutation can only skew toward (older version, newer rows) —
    # the live version then exceeds what we cache and the next acquire
    # re-resolves. The reverse pairing would cache stale rows under the new
    # version and stick. See get_workspace_servers_and_version. User-level
    # mutations fan the bump out to every workspace of the user, so the same
    # ordering argument covers the user reads below.
    rows, version = await get_workspace_servers_and_version(workspace_id)
    user_rows, connections, user_disabled_builtins = await asyncio.gather(
        list_enabled_user_servers(user_id),
        list_connections(user_id),
        list_user_builtin_disables(user_id),
    )

    # Short-circuit: nothing user-level and no workspace rows ⇒ the effective
    # set IS the built-in list (same objects, no copies).
    if not rows and not user_rows and not user_disabled_builtins:
        return ResolvedMCP(
            entries=tuple(
                ResolvedServer(config=s, origin=Origin.BUILTIN, state=State.ACTIVE)
                for s in builtin_servers
            ),
            version=version,
        )

    oauth_status_by_name = {
        c["server_name"]: ConnectionStatus(c["status"]) for c in connections
    }
    connection_by_server = {
        c["server_name"]: c
        for c in connections
        if oauth_status_by_name[c["server_name"]] is not ConnectionStatus.REVOKED
    }

    disabled_builtins: set[str] = set()
    tombstoned_user_names: set[str] = set()
    local_servers: list[MCPServerConfig] = []
    disabled_local_servers: list[MCPServerConfig] = []
    local_names: set[str] = set()
    for row in rows:
        if row["source"] == "builtin":
            # Disable-marker: only acts when it turns a built-in off.
            if not row["enabled"]:
                disabled_builtins.add(row["name"])
            continue
        if row["source"] == "user":
            # Tombstone: removes an inherited user server from THIS workspace.
            if not row["enabled"]:
                tombstoned_user_names.add(row["name"])
            continue
        # source == 'workspace'
        if row["name"] in builtin_name_set:
            # Backstop for the API's 409: a user server must never collide with
            # a built-in name. Skip + log; do not let it shadow the built-in.
            logger.warning(
                "[MCP] Skipping workspace server %r in workspace %s: name "
                "collides with a built-in (API should reject at write).",
                row["name"], workspace_id,
            )
            continue
        try:
            cfg = workspace_row_to_server_config(row)
        except Exception:
            logger.error(
                "[MCP] Failed to parse workspace server %r in workspace %s; "
                "skipping.", row["name"], workspace_id, exc_info=True,
            )
            continue
        # A workspace-local row shadows an inherited user server of the same
        # name whether enabled or not — a disabled local fork must not fall
        # back to running the inherited config the user explicitly forked.
        local_names.add(cfg.name)
        # Disabled workspace servers are excluded from the effective set (they
        # don't run), but carried separately so the API keeps a re-enable
        # toggle in the UI — mirrors the disabled built-in entries.
        if row["enabled"]:
            local_servers.append(cfg)
        else:
            disabled_local_servers.append(cfg)

    inherited_servers: list[MCPServerConfig] = []
    tombstoned_inherited: list[MCPServerConfig] = []
    shadowed_inherited: list[MCPServerConfig] = []
    for row in user_rows:
        name = row["name"]
        if name in builtin_name_set:
            # Built-in names are reserved at the user level too.
            logger.warning(
                "[MCP] Skipping user server %r for user %s: name collides "
                "with a built-in (API should reject at write).", name, user_id,
            )
            continue
        connection = connection_by_server.get(name)
        consented_url = connection.get("server_url") if connection else None
        if (
            connection
            and consented_url
            and not same_consented_url(consented_url, row.get("url"))
        ):
            # The catalog URL was edited since consent (or a write path missed
            # the revoke): the stored token was issued for a different host.
            # Never bind it — leave the server un-connected so no grant is
            # created and sync_oauth_grants retires any prior one; surface
            # needs_reauth so the UI prompts re-consent to the new URL. This is
            # defense-in-depth behind the edit-time revoke and the grant's own
            # server_url pinning.
            logger.warning(
                "[MCP] user %s server %r URL changed since consent "
                "(%s → %s); forcing reconnect",
                user_id, name, connection.get("server_url"), row.get("url"),
            )
            connection = None
            # Surface reconnect intent to the UI.
            oauth_status_by_name[name] = ConnectionStatus.NEEDS_REAUTH
        try:
            cfg = user_row_to_server_config(
                row,
                oauth_connection_id=(
                    connection["connection_id"] if connection else None
                ),
            )
        except Exception:
            logger.error(
                "[MCP] Failed to parse user server %r for user %s; skipping.",
                name, user_id, exc_info=True,
            )
            continue
        if name in local_names:
            shadowed_inherited.append(cfg)
        elif name in tombstoned_user_names:
            tombstoned_inherited.append(cfg)
        else:
            inherited_servers.append(cfg)

    inherited_servers.sort(key=lambda s: s.name)
    tombstoned_inherited.sort(key=lambda s: s.name)
    shadowed_inherited.sort(key=lambda s: s.name)
    local_servers.sort(key=lambda s: s.name)
    disabled_local_servers.sort(key=lambda s: s.name)

    plugin_name_by_server = {
        row["name"]: row["plugin_name"]
        for row in user_rows
        if row.get("plugin_name")
    }

    def _user_entry(cfg: MCPServerConfig, state: State) -> ResolvedServer:
        return ResolvedServer(
            config=cfg,
            origin=Origin.USER,
            state=state,
            oauth_status=oauth_status_by_name.get(cfg.name),
            plugin_name=plugin_name_by_server.get(cfg.name),
        )

    # Entry order IS the API's row order: the running set first (built-ins,
    # inherited, local), then the carried-but-not-running rows.
    entries: list[ResolvedServer] = [
        *(
            ResolvedServer(config=s, origin=Origin.BUILTIN, state=State.ACTIVE)
            for s in builtin_servers
            if s.name not in disabled_builtins
            and s.name not in user_disabled_builtins
        ),
        *(_user_entry(s, State.ACTIVE) for s in inherited_servers),
        *(
            ResolvedServer(config=s, origin=Origin.WORKSPACE, state=State.ACTIVE)
            for s in local_servers
        ),
        *(
            ResolvedServer(
                config=s,
                origin=Origin.BUILTIN,
                state=State.DISABLED,
                # The user scope wins the label when both disables exist: the
                # workspace toggle can't undo an account-wide disable anyway.
                disabled_scope=(
                    "user" if s.name in user_disabled_builtins else "workspace"
                ),
            )
            for s in builtin_servers
            if s.name in disabled_builtins or s.name in user_disabled_builtins
        ),
        *(_user_entry(s, State.TOMBSTONED) for s in tombstoned_inherited),
        *(
            ResolvedServer(config=s, origin=Origin.WORKSPACE, state=State.DISABLED)
            for s in disabled_local_servers
        ),
        *(_user_entry(s, State.SHADOWED) for s in shadowed_inherited),
    ]
    return ResolvedMCP(entries=tuple(entries), version=version)
