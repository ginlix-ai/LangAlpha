"""Per-workspace MCP configuration resolution — the single chokepoint.

Modeled on ``resolve_llm_config``. Merges the process-global built-in MCP
servers (from ``base_config.mcp.servers``), the user's enabled user-level
servers, and a workspace's selection rows into one deterministic effective set:

    effective = built-ins (config order)
                MINUS names disabled by a (source='builtin', enabled=false) row
                MINUS names disabled account-wide (server or owning bundle)
                PLUS  the organization's connectors (alphabetical;
                      ``services.org_connectors``)
                MINUS connectors disabled by a (source='org', enabled=false)
                      row, keyed by ``org_key(id)``
                PLUS  enabled user-level servers the policy lets a turn
                      use (alphabetical; ``services.mcp_sources``)
                MINUS names disabled by a (source='user', enabled=false) row

Servers are installed per user and selected per workspace, so a name means
the same server in every workspace of its user. Built-in names are reserved:
no other server can shadow one. An organization connector shadows a user
server of the same name, which is left untouched and comes back as it was
once the organization's list no longer names it.

User-level mutations bump every workspace of the user (one transaction), so
the single per-workspace ``mcp_config_version`` remains the only drift signal
sessions have to watch.

The merged list and the DB↔model converters are defined ONCE here so the API
effective-list endpoint and the sandbox-sync path can import the same logic
(no prompt/wrapper divergence).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlsplit

from ptc_agent.config.core import MCPServerConfig
from src.server.database.mcp_oauth import SERVABLE, ConnectionStatus
from src.server.database.org_keys import org_key
from src.server.database.shared_workspaces import is_holder
from src.server.services.account_contract import OrgConnector
from src.server.services.shared_workspaces.holder import withheld_from_holder
from src.server.services.tool_binding import (
    BindingPlan,
    inputs_from_row,
    resolve_plan,
)
from src.server.services.brokerage_capabilities import (
    denied_tools,
    group_keys_for,
    vendor_for_url,
)
from src.server.services.mcp_descriptions.prompt_text import (
    effective_description,
    profiles_by_server,
)
from src.server.services import org_connectors, platform_client

if TYPE_CHECKING:
    from src.server.models.mcp_server import OrgConnectorEntry

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
    ORG = "org"
    USER = "user"


class State(StrEnum):
    """How a server participates in one workspace's effective set.

    Only ``ACTIVE`` servers run. The other two are carried so the API can
    render a re-enable affordance.
    """

    ACTIVE = "active"
    DISABLED = "disabled"
    TOMBSTONED = "tombstoned"


@dataclass(frozen=True)
class ResolvedServer:
    """One server in a workspace's resolved set, with its partition labels."""

    config: MCPServerConfig
    origin: Origin
    state: State
    # OAuth connection status for a row a connection still claims, a repairable
    # one (``needs_reauth``) included, so consumers can tell "never OAuth" from
    # "OAuth but needs repair". A revoked connection is history and leaves this
    # None: the row is served by its own headers from then on, and the Plugins
    # catalog, which keeps the revoked status on its own rows, is where the
    # reconnect is offered. Set on ``USER``-origin entries, and on ``ORG``
    # entries that members sign in to, from the member's own connection.
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
    # The tools this connection's consent permits, or None for a server we
    # curate no capability groups for, which is every server that is not a
    # shipped brokerage. Empty is a real answer and distinct from None: it
    # means the user granted no group, so the server runs and offers nothing.
    denied_tools: frozenset[str] | None = None
    # How each granted tool reaches the model (sandbox wrapper, JSON tool,
    # or both), with the set the sandbox must not wrap and the set whose calls
    # stop for the user. None for a server nothing binds directly.
    binding_plan: BindingPlan | None = None
    # A connection-less http row whose stored bindings ask for a direct tool
    # while nothing has probed the address yet: the plan above is clamped to
    # the sandbox, and the first verdict is what releases it. False once a
    # probe has answered, however it answered.
    awaiting_probe: bool = False

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def host_side_oauth(self) -> bool:
        """Whether this server's tools are discovered host-side, never in-sandbox.

        True while a connection still claims the row, even a repairable one: the
        sandbox holds no vendor token, so a probe from there could only cache a
        junk failure. A revoked connection makes no such claim, so the row is
        served by its own headers and discovered like any header row.
        """
        return (
            self.origin is Origin.ORG
            or bool(self.config.oauth_connection_id)
            or (self.origin is Origin.USER and self.oauth_status is not None)
        )


@dataclass(frozen=True)
class ResolvedMCP:
    """The effective MCP server set for one workspace at one config version.

    ``entries`` is the single source of truth: one labelled row per server the
    workspace knows about, ordered so that filtering to ``ACTIVE`` yields the
    effective run order (built-ins in config order, then user servers
    alphabetically). The projections below are derived from it, so the
    partition can never disagree with itself. ``version`` is
    ``workspaces.mcp_config_version``.
    """

    entries: tuple[ResolvedServer, ...]
    version: int
    #: ``org_connectors.org_digest`` of the list this set was resolved from,
    #: so a warm computer can tell when the organization's list has moved.
    org_digest: str = ""
    #: The organization's connectors this set was resolved from, for the
    #: host-side discovery an acquire kicks for those not yet discovered.
    org_connectors: tuple[OrgConnector, ...] = ()

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
    def binding_plans_by_name(self) -> dict[str, BindingPlan]:
        """ACTIVE servers with a plan that binds at least one tool directly."""
        return {
            e.name: e.binding_plan
            for e in self.entries
            if e.state is State.ACTIVE
            and e.binding_plan is not None
            and e.binding_plan.direct
        }

    @cached_property
    def denied_tools_by_name(self) -> dict[str, frozenset[str]]:
        """Consent filters for the running set, keyed by server name.

        Only servers that have one, so a caller reads "absent" as "no policy"
        without having to know which vendors we curate. The value is what to
        subtract, never what to keep: a tool no capability group names is not in
        it, and stays visible.
        """
        return {
            e.name: e.denied_tools
            for e in self.entries
            if e.state is State.ACTIVE and e.denied_tools is not None
        }


@dataclass(frozen=True)
class ServerRef:
    """A workspace-addressable MCP name, classified for the mutation endpoints.

    ``row`` is the workspace row for tombstone/marker refs and the Plugins row
    for a live inherited one, whichever tier the ref resolved from.
    """

    name: str
    origin: Origin
    state: State
    row: dict | None = None


@dataclass(frozen=True, kw_only=True)
class OrgServerRef(ServerRef):
    """An organization connector's ref."""

    origin: Origin = Origin.ORG
    #: The connector's id, which its workspace marker is keyed by.
    connector_id: str


def builtin_names(*, enabled_only: bool = False) -> set[str]:
    """Names of the process-global built-in MCP servers (from agent_config),
    reserved across every tier; the routers and the resolver must agree on
    the set. A user server may take none of them, a disabled one included;
    an organization connector only stays off the ones a workspace resolves
    (``enabled_only``), as the resolver reserves for it."""
    from src.server.app import setup

    if setup.agent_config is None:
        return set()
    return {
        s.name for s in setup.agent_config.mcp.servers
        if not enabled_only or getattr(s, "enabled", True)
    }


async def account_disabled_builtins(user_id: str) -> frozenset[str]:
    """Built-in names this account has switched off, by either route.

    Two different writes produce the same subtraction: a per-server disable,
    and a disable of the bundle that ships the server. Both outrank every
    workspace, so every surface that asks "is this off for the whole account"
    has to mean both — asked once here rather than assembled per call site,
    which is how the workspace re-enable came to report success for a name it
    could not turn back on.
    """
    from src.server.database.account_disables import list_account_disables
    from src.server.services.plugins.bundled import enforcement_owners

    disables = await list_account_disables(user_id)
    if not disables.bundles:
        return disables.servers
    owned, _ = enforcement_owners().owned_by(disables.bundles)
    return disables.servers | owned


def reserved_catalog_names() -> set[str]:
    """Names a catalog row may not claim, whichever door it arrives through.

    Both sets are joined to a shipped definition by name and then shown wearing
    it, so the reservation has to hold at every writer rather than at the one
    the feature was built against: every create and import door mints a
    catalog row, and a name is only reserved if all of them agree it is.
    """
    from src.server.services.brokerages import brokerage_names

    return builtin_names() | brokerage_names()


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
    workspace_id: str, user_id: str, name: str, *, disabling: bool = False
) -> ServerRef | None:
    """Classify one MCP name for a workspace mutation, or ``None`` if unknown.

    Reads the mutable tiers directly (the organization's connectors, workspace
    rows, then the Plugins catalog) rather than going through
    ``resolve_mcp_config``: mutations need the raw row, not the merged set,
    and the built-in tier is checked by the caller against the process
    config. A stale ``source='builtin'`` marker only classifies as ``BUILTIN``
    once the Plugins tier has been ruled out. An organization connector's ref
    carries its ``source='org'`` marker as ``row``, None while it is on.

    ``disabling`` lets a switch-off go on while the organization's connectors
    are unknown: it can only narrow what the workspace reaches, and a connector
    is refused at the relay through the outage anyway. A name that then
    classifies as nothing stays unknown rather than not found.
    """
    from src.server.database.mcp_servers import (
        get_catalog_server,
        list_workspace_servers,
    )


    rows = {r["name"]: r for r in await list_workspace_servers(workspace_id)}
    # The organization's connectors outrank the user's own servers, so a name
    # they share addresses the connector.
    connectors = await org_connectors.org_connectors_for(user_id)
    if connectors is None and not disabling:
        raise platform_client.quota_service_unavailable()
    if connectors and (connector := org_connectors.connector_named(connectors, name)) is not None:
        marker = rows.get(org_key(connector.id))
        disabled = marker is not None and not marker.get("enabled")
        return OrgServerRef(
            name=name,
            state=State.DISABLED if disabled else State.ACTIVE,
            row=marker,
            connector_id=connector.id,
        )
    row = rows.get(name)
    source = (row or {}).get("source")
    if source == "user":
        # A (source='user', enabled=false) marker: this workspace's tombstone
        # for an inherited server.
        return ServerRef(name, Origin.USER, State.TOMBSTONED, row)

    catalog = await get_catalog_server(user_id, name)
    if catalog and catalog.get("enabled"):
        return ServerRef(name, Origin.USER, State.ACTIVE, catalog)
    if source == "builtin":
        return ServerRef(name, Origin.BUILTIN, State.DISABLED, row)
    if connectors is None:
        raise platform_client.quota_service_unavailable()
    return None


async def resolve_mcp_config(
    base_config,
    user_id: str,
    workspace_id: str,
) -> ResolvedMCP:
    """Resolve the effective MCP server set for ``workspace_id``.

    Built-ins come from ``base_config.mcp.servers`` (enabled ones, config
    order); a ``(source='builtin', enabled=false)`` row, an account-wide
    ``user_mcp_builtin_disables`` row, or a disable of the bundle that ships
    it removes a built-in by name; enabled user-level servers are inherited
    (alphabetical) unless tombstoned by a ``(source='user', enabled=false)``
    row. A workspace with zero rows AND zero user-level state returns the built-in
    objects unchanged (no copies) so the common case stays byte-identical
    downstream.
    """
    from src.server.database.mcp_oauth import list_connections
    from src.server.database.mcp_servers import get_workspace_servers_and_version
    from src.server.models.mcp_server import probe_ok, snapshot_probe
    from src.server.services.mcp_sources import user_tier_rows

    # Built-ins from the global config, enabled only, in declaration order.
    builtin_servers = [
        s for s in base_config.mcp.servers
        if getattr(s, "enabled", True)
    ]
    builtin_name_set = {s.name for s in builtin_servers}
    if is_holder(user_id):
        # Not a disable a member could undo: the holder's computer never
        # carries them, so they are left out rather than listed as off.
        builtin_servers = [s for s in builtin_servers if not withheld_from_holder(s)]

    # Version is read BEFORE the rows (READ COMMITTED, not a snapshot) so a
    # concurrent mutation can only skew toward (older version, newer rows) —
    # the live version then exceeds what we cache and the next acquire
    # re-resolves. The reverse pairing would cache stale rows under the new
    # version and stick. See get_workspace_servers_and_version. User-level
    # mutations fan the bump out to every workspace of the user, so the same
    # ordering argument covers the user reads below.
    from src.server.database.home_workspace import acting_for

    rows, version = await get_workspace_servers_and_version(
        workspace_id, rows_of=acting_for(user_id, workspace_id)
    )
    # A switched-off bundle is not a tier of its own: it expands into the
    # names of the built-ins it ships, and every rule below applies to them
    # unchanged. That expansion is what account_disabled_builtins does.
    # Each source beside the built-ins comes gated from ``mcp_sources``, None
    # when the policy that gates it is unknown: an outage must not resolve a
    # set the policy narrows as if it did not.
    # The organization's list reads nothing unless the deployment serves it.
    user_rows, connections, user_disabled_builtins, connectors = await asyncio.gather(
        user_tier_rows(user_id),
        list_connections(user_id),
        account_disabled_builtins(user_id),
        org_connectors.org_connectors_for(user_id),
    )
    if user_rows is None or connectors is None:
        raise platform_client.quota_service_unavailable()

    # Short-circuit: nothing user-level and no workspace rows ⇒ the effective
    # set IS the built-in list (same objects, no copies).
    if not rows and not user_rows and not user_disabled_builtins and not connectors:
        return ResolvedMCP(
            entries=tuple(
                ResolvedServer(config=s, origin=Origin.BUILTIN, state=State.ACTIVE)
                for s in builtin_servers
            ),
            version=version,
        )

    # A revoked connection is history, not a claim on the row: it neither binds
    # the row nor labels it OAuth, so the row resolves as the header row it is
    # and is planned and discovered off its own headers, the same reading the
    # catalog and the relay already take. (One connection per name is a table
    # invariant, so one filtered pass builds both maps.)
    oauth_status_by_name = {
        c["server_name"]: status
        for c in connections
        if (status := ConnectionStatus(c["status"])) is not ConnectionStatus.REVOKED
    }
    connection_by_server = {
        c["server_name"]: c
        for c in connections
        if c["server_name"] in oauth_status_by_name
    }

    # A stored ``direct`` override on a header-authenticated row is honoured
    # only once the host probe has said those headers work, because the egress
    # grant that makes the direct path callable is issued on the same verdict
    # (``grant_scope._probe_ok``). Honouring it earlier would take the tool out
    # of the sandbox with nothing to replace it, so an unprobed row clamps to
    # the sandbox exactly like a stdio one and the override goes live the
    # moment a verdict lands. Read only where some row could be in that
    # position: an OAuth row is decided by its connection instead.
    snapshots = None
    if any(
        row.get("transport") == "http" and row["name"] not in connection_by_server
        for row in user_rows
    ):
        from src.server.database.mcp_tool_schemas import get_user_tool_schemas
        from src.server.services.mcp_discovery import ToolSnapshotIndex

        snapshots = ToolSnapshotIndex(user_rows=await get_user_tool_schemas(user_id))

    disabled_builtins: set[str] = set()
    tombstoned_user_names: set[str] = set()
    disabled_org_keys: set[str] = set()
    for row in rows:
        if row["source"] == "builtin":
            # Disable-marker: only acts when it turns a built-in off.
            if not row["enabled"]:
                disabled_builtins.add(row["name"])
        elif row["source"] == "user" and not row["enabled"]:
            # Tombstone: removes an inherited user server from THIS workspace.
            tombstoned_user_names.add(row["name"])
        elif row["source"] == "org" and not row["enabled"]:
            # Keyed by the connector's id, so it survives a rename and never
            # shares a slot with a user server's tombstone of the same name.
            disabled_org_keys.add(row["name"])

    org_entries, org_names = _org_entries(
        connectors,
        user_id=user_id,
        reserved=builtin_name_set,
        disabled_keys=disabled_org_keys,
        connections=connections,
    )

    inherited_servers: list[MCPServerConfig] = []
    tombstoned_inherited: list[MCPServerConfig] = []
    user_row_by_name = {row["name"]: row for row in user_rows}
    profiles = await profiles_by_server(user_id, user_rows, connection_by_server)
    for row in user_rows:
        name = row["name"]
        if name in builtin_name_set:
            # Built-in names are reserved at the user level too.
            logger.warning(
                "[MCP] Skipping user server %r for user %s: name collides "
                "with a built-in (API should reject at write).", name, user_id,
            )
            continue
        if name in org_names:
            # Shadowed, never changed: the row and its tombstones stand as
            # they are, and it comes back once the organization drops the name.
            logger.warning(
                "[MCP] Skipping user server %r for user %s: an organization "
                "connector of the same name takes precedence.", name, user_id,
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
            # created and sync_egress_grants retires any prior one; surface
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
            # A prompt-only field, outside the discovery fingerprint, so this
            # changes what the agent reads and nothing about the server.
            cfg = cfg.model_copy(
                update={"description": effective_description(row, profiles.get(name))}
            )
        except Exception:
            logger.error(
                "[MCP] Failed to parse user server %r for user %s; skipping.",
                name, user_id, exc_info=True,
            )
            continue
        if name in tombstoned_user_names:
            tombstoned_inherited.append(cfg)
        else:
            inherited_servers.append(cfg)

    inherited_servers.sort(key=lambda s: s.name)
    tombstoned_inherited.sort(key=lambda s: s.name)

    plugin_name_by_server = {
        row["name"]: row["plugin_name"]
        for row in user_rows
        if row.get("plugin_name")
    }

    def _denied_tools(cfg: MCPServerConfig) -> frozenset[str] | None:
        """What consent refuses for this server, or None if we curate no groups.

        Keyed on the address, never on ``cfg.name``: the name is the user's to
        choose and to edit, so a row called anything else at a broker's host
        used to derive no policy at all, and one holding a broker's name while
        pointed elsewhere derived a policy against the wrong vendor's tools.
        The consented URL is what the token was issued for, and it is what the
        relay dials, so it is the only identity worth deriving from. Without a
        connection there is nothing consented to read, and the row's own URL is
        all there is -- which is the conservative direction anyway, since it
        denies that vendor's whole curation.

        A brokerage whose connection carries no record of consent is read as
        consent to nothing, so every curated tool is denied. That is the one
        place this policy is still strict: an absent record is a bug, not a
        vendor publishing something new, and the two must not be confused.

        Loud only where it is actually a bug, which is why the warning asks
        whether the vendor has groups rather than whether it has a policy. A
        brokerage listed but not yet curated has no groups to consent to, so it
        legitimately stores no record, and it denies nothing.
        """
        connection = connection_by_server.get(cfg.name)
        if connection is None:
            return denied_tools(vendor_for_url(cfg.url), ())
        vendor = vendor_for_url(connection.get("server_url"))
        capabilities = connection.get("granted_capabilities")
        if capabilities is None and group_keys_for(vendor):
            logger.warning(
                "[MCP] user %s connection %r has no recorded capability "
                "consent; refusing every curated tool until it is reconnected",
                user_id, cfg.name,
            )
        return denied_tools(vendor, capabilities or ())

    def _binding_plan(cfg: MCPServerConfig) -> tuple[BindingPlan, bool]:
        """Which path each granted tool takes, and whether a probe still gates
        it. Same identity rule as the denial: the consented URL, never the row
        name.

        A row with no connection is planned off its own address and consent to
        nothing: a header-authenticated server has no consent record to read,
        and a curated vendor's tools are all denied by that emptiness anyway.
        What survives is what the row itself asked for, and whether it may ask
        at all is the row's transport -- a stdio server has no address the
        relay could dial, so ``inputs_from_row`` clamps every one of its tools
        back to the sandbox -- and, for an http row, whether the probe has said
        the headers work.
        """
        connection = connection_by_server.get(cfg.name)
        inputs = inputs_from_row(user_row_by_name.get(cfg.name))
        if connection is not None:
            return resolve_plan(
                vendor_for_url(connection.get("server_url")),
                connection.get("granted_capabilities") or (),
                inputs,
            ), False
        # Planning off ``()`` is deliberate: a row that never went through a
        # consent screen is denied a curated vendor's whole curation, while
        # its uncurated tools still take the path the row asked for.
        vendor = vendor_for_url(cfg.url)
        plan = resolve_plan(vendor, (), inputs)
        snapshot = snapshots.snapshot(cfg) if snapshots is not None else None
        if not inputs.relayable or probe_ok(snapshot):
            return plan, False
        return (
            resolve_plan(vendor, (), replace(inputs, relayable=False)),
            bool(plan.direct) and snapshot_probe(snapshot) is None,
        )

    def _user_entry(cfg: MCPServerConfig, state: State) -> ResolvedServer:
        plan, awaiting_probe = _binding_plan(cfg)
        return ResolvedServer(
            config=cfg,
            origin=Origin.USER,
            state=state,
            oauth_status=oauth_status_by_name.get(cfg.name),
            plugin_name=plugin_name_by_server.get(cfg.name),
            denied_tools=_denied_tools(cfg),
            binding_plan=plan,
            awaiting_probe=awaiting_probe,
        )

    # Entry order IS the API's row order: the running set first (built-ins,
    # then inherited), then the carried-but-not-running rows.
    entries: list[ResolvedServer] = [
        *(
            ResolvedServer(config=s, origin=Origin.BUILTIN, state=State.ACTIVE)
            for s in builtin_servers
            if s.name not in disabled_builtins
            and s.name not in user_disabled_builtins
        ),
        *(e for e in org_entries if e.state is State.ACTIVE),
        *(_user_entry(s, State.ACTIVE) for s in inherited_servers),
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
        *(e for e in org_entries if e.state is not State.ACTIVE),
        *(_user_entry(s, State.TOMBSTONED) for s in tombstoned_inherited),
    ]
    return ResolvedMCP(
        entries=tuple(entries),
        version=version,
        org_digest=org_connectors.org_digest(connectors),
        org_connectors=tuple(connectors),
    )


def org_server_config(connector: OrgConnector) -> MCPServerConfig:
    """An organization connector as the server config a workspace runs.

    No headers and no env: whatever it authenticates with is resolved by the
    relay per call, and the sandbox entry generated from this is relay-bound,
    so the url is read on the host alone.
    """
    return MCPServerConfig(
        name=connector.name,
        enabled=True,
        description=connector.description,
        instruction=connector.instruction,
        transport="http",
        url=connector.url,
        source="org",
        org_connector_id=connector.id,
        org_revision=connector.revision,
    )


def _org_entries(
    connectors: Sequence[OrgConnector],
    *,
    user_id: str,
    reserved: set[str],
    disabled_keys: set[str],
    connections: list[dict],
) -> tuple[list[ResolvedServer], set[str]]:
    """The organization's connectors as resolved entries, alphabetical, with
    the names they claim from the user tier.

    The sandbox path only: no binding plan, so nothing is bound directly, and
    no consent filter, since no brokerage is among them. A connector members
    sign in to carries the member's own connection when its consent was given
    for the connector's current url.
    """
    by_key = {c["server_name"]: c for c in connections}
    entries: list[ResolvedServer] = []
    names: set[str] = set()
    usable = org_connectors.resolvable_connectors(connectors, reserved=reserved, user_id=user_id)
    for connector in sorted(usable, key=lambda c: c.name):
        names.add(connector.name)
        cfg = org_server_config(connector)
        status, connection_id = member_sign_in(connector, by_key.get(org_key(connector.id)))
        if connection_id is not None:
            cfg = cfg.model_copy(update={"oauth_connection_id": connection_id})
        entries.append(
            ResolvedServer(
                config=cfg,
                origin=Origin.ORG,
                state=(
                    State.DISABLED
                    if org_key(connector.id) in disabled_keys
                    else State.ACTIVE
                ),
                oauth_status=status,
            )
        )
    return entries, names


def member_sign_in(
    connector: OrgConnector, connection: Mapping[str, Any] | None
) -> tuple[ConnectionStatus | None, str | None]:
    """The member's sign-in to ``connector`` as a listing shows it, and the
    connection a workspace carries for it. A revoked connection is history,
    as for a user server: the member has not signed in. One given for
    another url must be given again."""
    if connector.auth != "member" or connection is None:
        return None, None
    current = ConnectionStatus(connection["status"])
    if current is ConnectionStatus.REVOKED:
        return None, None
    if not same_consented_url(connection.get("server_url"), connector.url):
        return ConnectionStatus.NEEDS_REAUTH, None
    return current, str(connection["connection_id"])


def servable_member_connection(connector: OrgConnector, connection: Any) -> bool:
    """Whether a call to ``connector`` may spend ``connection``: the member's
    own for it, live, and given for its current url."""
    return (
        connection is not None
        and connection.status in SERVABLE
        and connection.server_name == org_key(connector.id)
        and same_consented_url(connection.server_url, connector.url)
    )


async def member_listing(user_id: str) -> list[OrgConnectorEntry]:
    """The organization's connectors this member may use, with their own
    sign-in, discovery and the workspaces each is switched off in, read as a
    workspace's server list reads them so the two never disagree; 503 while
    the account cannot be read. Reads nothing more where none is usable."""
    from src.server.database.mcp_oauth import list_connections
    from src.server.database.mcp_servers import list_scope_markers_for_user
    from src.server.database.mcp_tool_schemas import get_user_tool_schemas
    from src.server.models.mcp_server import OrgConnectorEntry
    from src.server.services.mcp_discovery import ToolSnapshotIndex

    connectors = await org_connectors.org_connectors_for(user_id)
    if connectors is None:
        raise platform_client.quota_service_unavailable()
    reserved = builtin_names(enabled_only=True)
    usable = {
        c.name: c
        for c in org_connectors.resolvable_connectors(connectors, reserved=reserved, user_id=user_id)
    }
    if not usable:
        return []
    entries, _ = _org_entries(
        list(usable.values()), user_id=user_id, reserved=reserved,
        disabled_keys=set(), connections=await list_connections(user_id),
    )
    snapshots = ToolSnapshotIndex(user_rows=await get_user_tool_schemas(user_id))
    off: dict[str, list[str]] = {}
    for m in await list_scope_markers_for_user(user_id):
        if m["source"] == "org":
            off.setdefault(m["name"], []).append(m["workspace_id"])
    listing = []
    for entry in entries:
        connector = usable[entry.config.name]
        snapshot = snapshots.snapshot(entry.config) or {}
        found = snapshot.get("status")
        listing.append(
            OrgConnectorEntry(
                id=connector.id,
                name=connector.name,
                description=connector.description,
                auth=connector.auth,
                connection_status=entry.oauth_status,
                disabled_workspace_ids=sorted(off.get(org_key(connector.id), [])),
                discovery="ok" if found == "ok" else "error" if found == "error" else "pending",
                tool_count=len(snapshot.get("tools") or []) if found == "ok" else 0,
            )
        )
    return listing
